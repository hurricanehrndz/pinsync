#!/usr/bin/env bash
#
# Teardown of the Phase 0 test infrastructure created by up.sh, in reverse
# order: profile -> trust anchor -> role (inline policy + role) -> bucket.
# Idempotent: every delete tolerates the resource already being absent.
#
# Leaves hack/ra-test/state/ untouched (local CA + device certs survive).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PREFIX="pinsync-ra-test"

info() { printf '==> %s\n' "$*"; }
gone() { printf '    (absent) %s\n' "$*"; }
die() {
	printf 'ERROR: %s\n' "$*" >&2
	exit 1
}

AWS_REGION="${AWS_REGION:-$(aws configure get region || true)}"
[ -n "${AWS_REGION}" ] || die "AWS region is empty: set AWS_REGION or 'aws configure set region <region>'"

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
[ -n "${ACCOUNT_ID}" ] && [ "${ACCOUNT_ID}" != "None" ] || die "could not resolve AWS account id"

BUCKET="${PREFIX}-${ACCOUNT_ID}"

# 1. profile
delete_profile() {
	local arn id
	arn="$(aws rolesanywhere list-profiles \
		--query "profiles[?name=='${PREFIX}'].profileArn | [0]" --output text)"
	if [ -z "${arn}" ] || [ "${arn}" = "None" ]; then
		gone "profile ${PREFIX}"
		return
	fi
	id="${arn##*/}"
	info "deleting profile ${PREFIX} (${id})"
	aws rolesanywhere delete-profile --profile-id "${id}" >/dev/null
}

# 2. trust anchor
delete_trust_anchor() {
	local arn id
	arn="$(aws rolesanywhere list-trust-anchors \
		--query "trustAnchors[?name=='${PREFIX}'].trustAnchorArn | [0]" --output text)"
	if [ -z "${arn}" ] || [ "${arn}" = "None" ]; then
		gone "trust anchor ${PREFIX}"
		return
	fi
	id="${arn##*/}"
	info "deleting trust anchor ${PREFIX} (${id})"
	aws rolesanywhere delete-trust-anchor --trust-anchor-id "${id}" >/dev/null
}

# 3. IAM role (inline policy first, then the role)
delete_role() {
	if ! aws iam get-role --role-name "${PREFIX}" >/dev/null 2>&1; then
		gone "IAM role ${PREFIX}"
		return
	fi
	local pol
	for pol in $(aws iam list-role-policies --role-name "${PREFIX}" \
		--query "PolicyNames" --output text); do
		info "deleting inline policy ${pol} from role ${PREFIX}"
		aws iam delete-role-policy --role-name "${PREFIX}" --policy-name "${pol}"
	done
	info "deleting IAM role ${PREFIX}"
	aws iam delete-role --role-name "${PREFIX}"
}

# 4. bucket (empty then delete)
delete_bucket() {
	if ! aws s3api head-bucket --bucket "${BUCKET}" >/dev/null 2>&1; then
		gone "bucket ${BUCKET}"
		return
	fi
	info "emptying bucket ${BUCKET}"
	aws s3 rm "s3://${BUCKET}" --recursive >/dev/null
	info "deleting bucket ${BUCKET}"
	aws s3api delete-bucket --bucket "${BUCKET}" --region "${AWS_REGION}" >/dev/null
}

info "region=${AWS_REGION} account=${ACCOUNT_ID} bucket=${BUCKET}"
delete_profile
delete_trust_anchor
delete_role
delete_bucket
info "done (state/ left untouched)."
