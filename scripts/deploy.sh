#!/usr/bin/env bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# Full CDK deployment — preflight, build, synth, deploy.
# CDK handles stack ordering via addDependency() in bin/app.ts.
set -euo pipefail

ENV="${1:-dev}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Optional context overrides via environment variables:
#   SCL_PREFIX=myproj  make deploy-dev
#   SCL_VPC_ID=vpc-abc make deploy-dev
# Custom domains (all-or-nothing — set all five or none):
#   SCL_UI_DOMAIN, SCL_UI_CERT_ARN (us-east-1),
#   SCL_API_DOMAIN, SCL_API_CERT_ARN (API region), SCL_HOSTED_ZONE_ID
# Database scan enrichment timeout (minutes; raise for very large sources):
#   SCL_DB_SCAN_ENRICHMENT_TIMEOUT_MINUTES=180 make deploy-dev
# Lambda reserved concurrency (default 5; set 0 to disable reserving on
# accounts whose Lambda concurrent-executions quota is the reduced default 10):
#   SCL_LAMBDA_RESERVED_CONCURRENCY=0 make deploy-dev
# SMUS admin principal(s) — required, comma-separated IAM role/user ARN(s)
# that human admins federate into (e.g. your IAM Identity Center
# permission-set role): SCL_SMUS_ADMIN_ARNS=arn:aws:iam::123456789012:role/... make deploy-dev
CONTEXT="--context env=$ENV"
[ -n "${SCL_PREFIX:-}" ]        && CONTEXT="$CONTEXT --context resource_prefix=$SCL_PREFIX"
[ -n "${SCL_PROJECT_TAG:-}" ]   && CONTEXT="$CONTEXT --context project_tag=$SCL_PROJECT_TAG"
[ -n "${SCL_VPC_ID:-}" ]        && CONTEXT="$CONTEXT --context vpc_id=$SCL_VPC_ID"
[ -n "${SCL_UI_DOMAIN:-}" ]     && CONTEXT="$CONTEXT --context ui_domain=$SCL_UI_DOMAIN"
[ -n "${SCL_UI_CERT_ARN:-}" ]   && CONTEXT="$CONTEXT --context ui_cert_arn=$SCL_UI_CERT_ARN"
[ -n "${SCL_API_DOMAIN:-}" ]    && CONTEXT="$CONTEXT --context api_domain=$SCL_API_DOMAIN"
[ -n "${SCL_API_CERT_ARN:-}" ]  && CONTEXT="$CONTEXT --context api_cert_arn=$SCL_API_CERT_ARN"
[ -n "${SCL_HOSTED_ZONE_ID:-}" ] && CONTEXT="$CONTEXT --context hosted_zone_id=$SCL_HOSTED_ZONE_ID"
[ -n "${SCL_DB_SCAN_ENRICHMENT_TIMEOUT_MINUTES:-}" ] && CONTEXT="$CONTEXT --context dbScanEnrichmentTimeoutMinutes=$SCL_DB_SCAN_ENRICHMENT_TIMEOUT_MINUTES"
[ -n "${SCL_LAMBDA_RESERVED_CONCURRENCY:-}" ] && CONTEXT="$CONTEXT --context lambda_reserved_concurrency=$SCL_LAMBDA_RESERVED_CONCURRENCY"
[ -n "${SCL_SMUS_ADMIN_ARNS:-}" ] && CONTEXT="$CONTEXT --context smus_admin_principal_arns=$SCL_SMUS_ADMIN_ARNS"

# ── Preflight: SMUS admin principal ──────────────────────────────────────
# NamespaceStack falls back to arn:aws:iam::<account>:role/Admin when
# SCL_SMUS_ADMIN_ARNS is unset. That role is an Amazon-internal account
# convention, not something AWS or this project creates, so on any other
# account the fallback fails deploy several stacks deep (DomainLoginRole
# CREATE_FAILED, then an orphaned DataZone domain on retry). Check it here,
# with real credentials, before CDK ever runs.
if [ -z "${SCL_SMUS_ADMIN_ARNS:-}" ]; then
  ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "")
  ADMIN_ROLE_EXISTS=""
  if [ -n "$ACCOUNT_ID" ]; then
    ADMIN_ROLE_EXISTS=$(aws iam get-role --role-name Admin --query "Role.RoleName" --output text 2>/dev/null || echo "")
  fi
  if [ -z "$ADMIN_ROLE_EXISTS" ]; then
    echo "ERROR: No SMUS admin principal configured, and this account has no IAM role named 'Admin' to fall back to." >&2
    echo "Set SCL_SMUS_ADMIN_ARNS to the IAM role/user ARN(s) that human admins federate into," >&2
    echo "e.g. for an IAM Identity Center account, your permission set's federated role:" >&2
    echo "  SCL_SMUS_ADMIN_ARNS=arn:aws:iam::${ACCOUNT_ID:-<account>}:role/aws-reserved/sso.amazonaws.com/<region>/AWSReservedSSO_AdministratorAccess_<suffix> make deploy-dev" >&2
    exit 1
  fi
  echo "No SCL_SMUS_ADMIN_ARNS set — falling back to this account's existing 'Admin' role."
fi

# ── Resolve VPC peering context from test-databases stack (if deployed) ──
# Prefix default matches the CDK app (see DEFAULT_RESOURCE_PREFIX).
TEST_STACK_NAME="${SCL_PREFIX:-coa}-integ-test-databases"
if [ -z "${SCL_JDBC_PEER_VPC_ID:-}" ]; then
  SCL_JDBC_PEER_VPC_ID=$(aws cloudformation describe-stacks \
    --stack-name "$TEST_STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='VpcId'].OutputValue" \
    --output text 2>/dev/null || echo "")
fi
if [ -z "${SCL_JDBC_PEER_CIDRS:-}" ]; then
  SCL_JDBC_PEER_CIDRS=$(aws cloudformation describe-stacks \
    --stack-name "$TEST_STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='VpcCidr'].OutputValue" \
    --output text 2>/dev/null || echo "")
fi
if [ -n "${SCL_JDBC_PEER_VPC_ID:-}" ] && [ "$SCL_JDBC_PEER_VPC_ID" != "None" ] \
  && [ -n "${SCL_JDBC_PEER_CIDRS:-}" ] && [ "$SCL_JDBC_PEER_CIDRS" != "None" ]; then
  echo "Peering into test VPC ${SCL_JDBC_PEER_VPC_ID} (${SCL_JDBC_PEER_CIDRS})"
  CONTEXT="$CONTEXT --context jdbc_peer_vpc_id=$SCL_JDBC_PEER_VPC_ID --context jdbc_peer_cidrs=$SCL_JDBC_PEER_CIDRS"
else
  # The stack is optional, so skipping is normal — but say so. Silence here is
  # indistinguishable from a prefix mismatch resolving the wrong stack name, which
  # is how this went unnoticed while the default was 'scl'.
  #
  # "Skipping" is also the wrong intuition, hence the second line. CDK context is
  # not persisted between deploys: with these keys absent, serve-stack and the
  # connector SGs compute an EMPTY remote-CIDR set and CloudFormation deletes any
  # cross-VPC egress rules a previous deploy created. The next JDBC query then
  # hangs 60s on a silently dropped SYN, and the stack diff shows only security
  # group rules being removed — nothing naming peering. Recovering needs the test
  # stack present, a redeploy, and tests/cdk/scripts/connect-cross-network.sh.
  #
  # CI does not have this failure mode: ci/mainline.yml's deploy-dev resolves the
  # same outputs but `exit 1`s when they are missing, and `needs: deploy-test-stack`
  # guarantees they exist. Only this human-facing path continues past it.
  echo "No JDBC peering context (stack ${TEST_STACK_NAME} not found or has no VPC outputs) — skipping"
  echo "  WARNING: this REMOVES any cross-VPC JDBC egress rules an earlier deploy created."
  echo "           Deploy ${TEST_STACK_NAME} first if this environment runs JDBC integ tests."
fi

# ponytail: CDK_DOCKER selection — only pick Finch if its daemon is actually
# reachable, matching preflight's check. Otherwise CDK falls back to docker.
# (A bare `command -v finch` would pin a down daemon and fail preflight even
# when Docker is up.)
if [ -z "${CDK_DOCKER:-}" ]; then
  if command -v finch >/dev/null 2>&1 && finch info >/dev/null 2>&1; then
    export CDK_DOCKER=finch
  fi
fi

echo "=== Deploying to $ENV ==="
echo "CDK context: $CONTEXT"

cd "$REPO_ROOT"

# ── Preflight ────────────────────────────────────────────────────────────
echo ""
./scripts/preflight-deploy.sh

# ── Build ────────────────────────────────────────────────────────────────
echo ""
echo "Building all packages..."
pnpm nx run-many -t build

# ── CDK synth + deploy ───────────────────────────────────────────────────
echo ""
echo "Synthesizing CDK stacks..."
pnpm --filter coa-infra exec cdk synth $CONTEXT >/dev/null

echo ""
echo "Deploying all stacks..."
pnpm --filter coa-infra exec cdk deploy --all $CONTEXT --require-approval=never

echo ""
echo "=== Deployment to $ENV complete ==="
