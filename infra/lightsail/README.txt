Lightsail validation deployment
==============================

Region: eu-west-1 (Ireland). Instance: prediction-bot-ie-2gb.
Bundle: small_3_0, Linux, 2 GB RAM, 2 vCPU, USD 12/month before taxes.
No paid snapshots, managed databases, load balancers or static IP reservations.
Stopping this instance does not stop Lightsail billing; delete it to end charges.

Terraform state and terraform.tfvars.json are local and excluded from Git.
Keep them to manage this deployment; never apply the older ECS/RDS configuration.
The imported public SSH key is managed by Terraform. The private key is stored
outside this directory at .tmp/lightsail/bot.key and must be retained securely.
If the administrator's public IP changes, update admin_cidr and apply Terraform.
Public access is restricted to SSH from that /32. App ports bind to loopback.
The public IP can change after stop/start; query Terraform refresh or AWS first.

Run Terraform from this directory:
  terraform init
  terraform plan
  terraform apply

bootstrap.sh installs Docker and 1 GB emergency swap. Lightsail wraps user data
in a POSIX shell, so the bootstrap must remain POSIX-compatible. Bootstrap edits
are applied manually over SSH to avoid replacing an existing server.

compose.yaml uses the exact Docker images exported from the running local stack
on 2026-11-06. These are private local images, not published registry artifacts.
API and trading images differ, matching their local running container versions.
No source-code edits or local uncommitted changes are included in these images.
Image defaults retain these versions until a GitHub deployment writes a Compose
override with images tagged by the tested commit SHA.

prepare-validation.sh creates a new local database password and control key on
the server. It does not copy venue credentials. Monitoring starts disabled and
the journal is empty. Never enable live trading before reconciling holdings and
ensuring that the local and remote stacks cannot submit concurrently.

From the repository root, after confirming the current public IP:
  ssh -i .tmp/lightsail/bot.key -o UserKnownHostsFile=.tmp/lightsail/known_hosts -o StrictHostKeyChecking=yes -N -L 18080:127.0.0.1:8080 ubuntu@SERVER_IP
Then open http://127.0.0.1:18080 for the remote dashboard.

Manual inventory imports are database/journal adjustments, not blockchain
transactions. They require paused trading and no pending executions. Polymarket
relayer credentials are required separately for gas-sponsored chain operations;
manual imports do not require sending those operations. Predict on-chain
cancellation is disabled in this deployment.

Set LIMITLESS_ENABLED=false for this Polymarket/Predict deployment. This removes
Limitless credential requirements, monitoring subscriptions, execution and
inventory adapters, health gating and automatic redemption. Historical Limitless
records remain in PostgreSQL. The default is true for existing deployments.

2026-11-06 access check: Polymarket's geoblock endpoint reported blocked=true for
both server IPv4 and IPv6 in Ireland. Its API documentation separately described
Ireland as frontend-only restricted. That probe did not validate order submission.

GitHub Actions deployment
=========================

.github/workflows/lightsail.yml is manually dispatched from main. Its default
mode runs Checks and verifies AWS, SSH and application health without restarting
containers. Select the deploy input to build images on GitHub and deploy over SSH.
API and trading use the same backend image with separate entry points. Builds
contain the selected committed source, not local uncommitted changes.

Apply Terraform to create the GitHub OIDC provider and the deployment role.
If the AWS account already has this OIDC provider, import it instead of creating
a duplicate. The role can query instance metadata in this account's Ireland region
and modify only this deployment instance's port rules;
it cannot create instances or read venue credentials through AWS APIs.

In repository Settings > Environments, create lightsail and restrict deployment
branches to main. Add the variable LIGHTSAIL_ROLE_ARN from Terraform's
github_deploy_role_arn output. Add these environment secrets:
  LIGHTSAIL_SSH_KEY: a dedicated deployment key authorized for ubuntu.
  LIGHTSAIL_KNOWN_HOSTS: the verified server entry from SSH known_hosts.
The deployment SSH key gives access to the server, including its .env. Treat it
as privileged. Do not store venue keys in GitHub; keep them in the server .env.
Changing the instance IP requires updating the verified known_hosts entry.

Commit/push the workflow and infra files, then Actions > Deploy Lightsail >
Run workflow > main. Leave deploy unchecked for a connection verification, or
check it for a real deployment. Checks must pass before AWS/SSH credentials are available.
The runner temporarily opens TCP 22 for its own IPv4 /32 and closes that rule
in an always step. If GitHub terminates the runner before cleanup, remove that
temporary rule manually. Do not run Terraform apply during deployment.

Deployment requires a healthy, paused trading runtime with zero active
executions. Keep trading paused until the workflow finishes. It never enables
trading or overwrites .env. Existing database and journal volumes are retained.
Only api, trading and frontend are recreated. Health failures fail the job;
there is no automatic database rollback. Previous Compose files are retained
under releases/<commit>/, along with old images for operator-led recovery.
Remove old release images periodically after validating a successful release;
no automatic pruning deletes rollback images. GitHub Actions usage is billed
separately from AWS and depends on the repository plan and included minutes.

References:
https://docs.github.com/en/actions/how-tos/secure-your-work/security-harden-deployments/oidc-in-aws
https://docs.aws.amazon.com/service-authorization/latest/reference/list_lightsail.html
