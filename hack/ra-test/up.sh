#!/usr/bin/env bash
#
# Phase 0 test infrastructure for pinsync: stand up an IAM Roles Anywhere trust
# anchor + profile + role and a test S3 bucket, plus a local test CA and two
# device certificates. Fully idempotent: every step checks for existence before
# creating, so a second run is a complete no-op.
#
# Generated state (CA key/cert, device certs, PKCS#12 bundles, env file) is
# written to hack/ra-test/state/ which is gitignored.

set -euo pipefail

# --- locations ---------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_DIR="${SCRIPT_DIR}/state"
mkdir -p "${STATE_DIR}"

# --- constants ---------------------------------------------------------------
PREFIX="pinsync-ra-test"
P12_PASS="pinsync-ra-test"
CA_KEY="${STATE_DIR}/ca.key"
CA_PEM="${STATE_DIR}/ca.pem"
ENV_FILE="${STATE_DIR}/ra-test.env"

# --- helpers -----------------------------------------------------------------
info() { printf '==> %s\n' "$*"; }
skip() { printf '    (exists) %s\n' "$*"; }
die() {
	printf 'ERROR: %s\n' "$*" >&2
	exit 1
}

# --- preconditions -----------------------------------------------------------
AWS_REGION="${AWS_REGION:-$(aws configure get region || true)}"
[ -n "${AWS_REGION}" ] || die "AWS region is empty: set AWS_REGION or 'aws configure set region <region>'"

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
[ -n "${ACCOUNT_ID}" ] && [ "${ACCOUNT_ID}" != "None" ] || die "could not resolve AWS account id"

BUCKET="${PREFIX}-${ACCOUNT_ID}"

# =============================================================================
# 1. Test CA
# =============================================================================
create_ca() {
	if [ -f "${CA_KEY}" ] && [ -f "${CA_PEM}" ]; then
		skip "test CA (${CA_PEM})"
		return
	fi
	info "creating test CA"
	openssl genrsa -out "${CA_KEY}" 4096
	openssl req -x509 -new -key "${CA_KEY}" -sha256 -days 3650 \
		-subj "/CN=Pinsync Test CA" \
		-addext "basicConstraints=critical,CA:TRUE" \
		-addext "keyUsage=critical,keyCertSign,cRLSign" \
		-out "${CA_PEM}"
	chmod 600 "${CA_KEY}"
}

# =============================================================================
# 2. Trust anchor
# =============================================================================
TRUST_ANCHOR_ARN=""
TRUST_ANCHOR_ID=""

create_trust_anchor() {
	# find existing by name
	TRUST_ANCHOR_ARN="$(aws rolesanywhere list-trust-anchors \
		--query "trustAnchors[?name=='${PREFIX}'].trustAnchorArn | [0]" \
		--output text)"

	if [ -n "${TRUST_ANCHOR_ARN}" ] && [ "${TRUST_ANCHOR_ARN}" != "None" ]; then
		skip "trust anchor ${PREFIX} (${TRUST_ANCHOR_ARN})"
	else
		info "creating trust anchor ${PREFIX}"
		local source_json
		source_json="$(jq -n --arg cert "$(cat "${CA_PEM}")" \
			'{sourceType:"CERTIFICATE_BUNDLE", sourceData:{x509CertificateData:$cert}}')"
		TRUST_ANCHOR_ARN="$(aws rolesanywhere create-trust-anchor \
			--name "${PREFIX}" \
			--source "${source_json}" \
			--enabled \
			--query "trustAnchor.trustAnchorArn" --output text)"
		[ -n "${TRUST_ANCHOR_ARN}" ] && [ "${TRUST_ANCHOR_ARN}" != "None" ] ||
			die "trust anchor creation returned no ARN"
	fi

	TRUST_ANCHOR_ID="${TRUST_ANCHOR_ARN##*/}"

	# ensure enabled (create-trust-anchor defaults to disabled on older APIs)
	local enabled
	enabled="$(aws rolesanywhere get-trust-anchor --trust-anchor-id "${TRUST_ANCHOR_ID}" \
		--query "trustAnchor.enabled" --output text)"
	if [ "${enabled}" != "True" ]; then
		info "enabling trust anchor ${PREFIX}"
		aws rolesanywhere enable-trust-anchor --trust-anchor-id "${TRUST_ANCHOR_ID}" >/dev/null
	fi
}

# =============================================================================
# 3. IAM role (trust policy + inline S3 policy)
# =============================================================================
ROLE_ARN=""

create_role() {
	local trust_policy inline_policy
	trust_policy="$(jq -n --arg ta "${TRUST_ANCHOR_ARN}" '{
		Version: "2012-10-17",
		Statement: [{
			Effect: "Allow",
			Principal: { Service: "rolesanywhere.amazonaws.com" },
			Action: ["sts:AssumeRole", "sts:TagSession", "sts:SetSourceIdentity"],
			Condition: { ArnEquals: { "aws:SourceArn": $ta } }
		}]
	}')"

	if aws iam get-role --role-name "${PREFIX}" >/dev/null 2>&1; then
		skip "IAM role ${PREFIX}"
		info "updating role trust policy"
		aws iam update-assume-role-policy --role-name "${PREFIX}" \
			--policy-document "${trust_policy}"
	else
		info "creating IAM role ${PREFIX}"
		aws iam create-role --role-name "${PREFIX}" \
			--assume-role-policy-document "${trust_policy}" \
			--description "pinsync Roles Anywhere test role" >/dev/null
	fi

	ROLE_ARN="$(aws iam get-role --role-name "${PREFIX}" --query "Role.Arn" --output text)"

	inline_policy="$(jq -n --arg b "arn:aws:s3:::${BUCKET}" '{
		Version: "2012-10-17",
		Statement: [{
			Effect: "Allow",
			Action: ["s3:ListBucket", "s3:GetObject"],
			Resource: [$b, ($b + "/*")]
		}]
	}')"

	info "putting inline S3 policy on role"
	aws iam put-role-policy --role-name "${PREFIX}" \
		--policy-name "${PREFIX}-s3" \
		--policy-document "${inline_policy}"
}

# =============================================================================
# 4. Roles Anywhere profile
# =============================================================================
PROFILE_ARN=""
PROFILE_ID=""

create_profile() {
	PROFILE_ARN="$(aws rolesanywhere list-profiles \
		--query "profiles[?name=='${PREFIX}'].profileArn | [0]" \
		--output text)"

	if [ -n "${PROFILE_ARN}" ] && [ "${PROFILE_ARN}" != "None" ]; then
		skip "profile ${PREFIX} (${PROFILE_ARN})"
	else
		info "creating profile ${PREFIX}"
		PROFILE_ARN="$(aws rolesanywhere create-profile \
			--name "${PREFIX}" \
			--role-arns "${ROLE_ARN}" \
			--enabled \
			--query "profile.profileArn" --output text)"
		[ -n "${PROFILE_ARN}" ] && [ "${PROFILE_ARN}" != "None" ] ||
			die "profile creation returned no ARN"
	fi

	PROFILE_ID="${PROFILE_ARN##*/}"

	local enabled
	enabled="$(aws rolesanywhere get-profile --profile-id "${PROFILE_ID}" \
		--query "profile.enabled" --output text)"
	if [ "${enabled}" != "True" ]; then
		info "enabling profile ${PREFIX}"
		aws rolesanywhere enable-profile --profile-id "${PROFILE_ID}" >/dev/null
	fi
}

# =============================================================================
# 5. Test bucket
# =============================================================================
create_bucket() {
	if aws s3api head-bucket --bucket "${BUCKET}" >/dev/null 2>&1; then
		skip "bucket ${BUCKET}"
		return
	fi
	info "creating bucket ${BUCKET}"
	# us-east-1 must NOT be given a LocationConstraint.
	if [ "${AWS_REGION}" = "us-east-1" ]; then
		aws s3api create-bucket --bucket "${BUCKET}" --region "${AWS_REGION}" >/dev/null
	else
		aws s3api create-bucket --bucket "${BUCKET}" --region "${AWS_REGION}" \
			--create-bucket-configuration "LocationConstraint=${AWS_REGION}" >/dev/null
	fi
}

# =============================================================================
# 6. Device certificates (RSA 2048 + ECDSA P-256) -> PKCS#12
# =============================================================================
# issue_device_cert <slug> <cn> <keygen-cmd...>
issue_device_cert() {
	local slug="$1" cn="$2"
	shift 2
	local key="${STATE_DIR}/${slug}.key"
	local crt="${STATE_DIR}/${slug}.pem"
	local csr="${STATE_DIR}/${slug}.csr"
	local p12="${STATE_DIR}/${slug}.p12"

	if [ -f "${crt}" ] && [ -f "${p12}" ]; then
		skip "device cert ${slug}"
		return
	fi

	info "issuing device cert ${slug} (CN=${cn})"
	# generate private key via the provided command (writes to ${key})
	"$@"
	chmod 600 "${key}"

	openssl req -new -key "${key}" -subj "/CN=${cn}" -out "${csr}"
	openssl x509 -req -in "${csr}" \
		-CA "${CA_PEM}" -CAkey "${CA_KEY}" -CAcreateserial \
		-days 365 -sha256 \
		-extfile <(printf 'keyUsage=critical,digitalSignature\n') \
		-out "${crt}"
	rm -f "${csr}"

	# -legacy: macOS `security import` rejects OpenSSL 3's default PKCS#12
	# encryption (PBES2/AES-256/hmacSHA256) with "MAC verification failed";
	# the legacy 3DES/SHA1 scheme imports on both Keychain and Windows CNG.
	openssl pkcs12 -export -legacy -out "${p12}" \
		-inkey "${key}" -in "${crt}" -certfile "${CA_PEM}" \
		-name "${cn}" -passout "pass:${P12_PASS}"
}

gen_rsa_key() { openssl genrsa -out "${STATE_DIR}/device-rsa.key" 2048; }
gen_ec_key() { openssl ecparam -name prime256v1 -genkey -noout -out "${STATE_DIR}/device-ec.key"; }

create_device_certs() {
	issue_device_cert "device-rsa" "pinsync-test-device-rsa" gen_rsa_key
	issue_device_cert "device-ec" "pinsync-test-device-ec" gen_ec_key
}

# =============================================================================
# 7. env file
# =============================================================================
write_env() {
	info "writing ${ENV_FILE}"
	cat >"${ENV_FILE}" <<EOF
# Generated by hack/ra-test/up.sh -- do not edit by hand.
RA_TRUST_ANCHOR_ARN=${TRUST_ANCHOR_ARN}
RA_PROFILE_ARN=${PROFILE_ARN}
RA_ROLE_ARN=${ROLE_ARN}
RA_BUCKET=${BUCKET}
AWS_REGION=${AWS_REGION}
EOF
}

print_import_instructions() {
	cat <<EOF

Device certificate PKCS#12 bundles (passphrase: ${P12_PASS})
  ${STATE_DIR}/device-rsa.p12
  ${STATE_DIR}/device-ec.p12

Import on macOS:
  security import ${STATE_DIR}/device-rsa.p12 -k login.keychain-db -P ${P12_PASS}
  security import ${STATE_DIR}/device-ec.p12 -k login.keychain-db -P ${P12_PASS}

Import on Windows (PowerShell):
  Import-PfxCertificate -FilePath ${STATE_DIR}/device-rsa.p12 -CertStoreLocation Cert:\\CurrentUser\\My -Password (ConvertTo-SecureString ${P12_PASS} -AsPlainText -Force)
  Import-PfxCertificate -FilePath ${STATE_DIR}/device-ec.p12 -CertStoreLocation Cert:\\CurrentUser\\My -Password (ConvertTo-SecureString ${P12_PASS} -AsPlainText -Force)
EOF
}

# --- main --------------------------------------------------------------------
info "region=${AWS_REGION} account=${ACCOUNT_ID} bucket=${BUCKET}"
create_ca
create_trust_anchor
create_role
create_profile
create_bucket
create_device_certs
write_env
print_import_instructions
info "done."
