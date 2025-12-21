# Tonbo on Cloudflare Workers

This example demonstrates running Tonbo as an embedded database on [Cloudflare Workers](https://workers.cloudflare.com/), using either Cloudflare R2 or any S3-compatible storage backend.

## Prerequisites

- [Rust](https://rustup.rs/) (1.90+ recommended)
- [Node.js](https://nodejs.org/) (for wrangler CLI)
- [wrangler](https://developers.cloudflare.com/workers/wrangler/install-and-update/) CLI: `npm install -g wrangler`
- For local testing: [Docker](https://docker.com/) (to run LocalStack)

## Quick Start

### 1. Local Testing with LocalStack

Start LocalStack (S3-compatible local storage):

```bash
docker run -d --name localstack -p 4566:4566 -e SERVICES=s3 localstack/localstack:3.0.2
```

Create a test bucket:

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url=http://localhost:4566 s3api create-bucket \
  --bucket tonbo-test --region us-east-1
```

Set CORS (required for browser/WASM access):

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url=http://localhost:4566 s3api put-bucket-cors \
  --bucket tonbo-test \
  --cors-configuration '{"CORSRules":[{"AllowedOrigins":["*"],"AllowedMethods":["GET","PUT","HEAD","DELETE"],"AllowedHeaders":["*"],"ExposeHeaders":["ETag"],"MaxAgeSeconds":300}]}'
```

Create `.dev.vars` with test credentials:

```bash
cat > .dev.vars << 'EOF'
TONBO_S3_ACCESS_KEY=test
TONBO_S3_SECRET_KEY=test
EOF
```

Run the worker locally:

```bash
npx wrangler dev
```

Test it:

```bash
# Write and read back data
curl -X POST http://localhost:8787/write
# Output:
# Wrote 2 rows (alice=100, bob=200) to Tonbo DB.
# Read back: alice = 100
# Note: Cloudflare Workers have subrequest limits...
```

### 2. Deploy to Cloudflare with R2

#### Create R2 bucket via CLI

```bash
# Login to Cloudflare (opens browser for authentication)
npx wrangler login

# Create a new R2 bucket
npx wrangler r2 bucket create your-bucket-name

# Note your account ID from the output, or get it with:
npx wrangler whoami
```

#### Create R2 API Token (Dashboard required)

R2 API tokens must be created in the Cloudflare dashboard:

1. Go to [Cloudflare Dashboard](https://dash.cloudflare.com/) > R2 > Overview
2. Click "Manage R2 API Tokens" in the right sidebar
3. Click "Create API token"
4. Configure the token:
   - **Token name**: e.g., "tonbo-worker"
   - **Permissions**: Object Read & Write
   - **Specify bucket(s)**: Select your bucket (e.g., "your-bucket-name")
5. Click "Create API Token"
6. **IMPORTANT**: Copy both the Access Key ID and Secret Access Key immediately (the secret is only shown once!)

#### Configure wrangler.toml

Update `wrangler.toml` with your R2 endpoint:

```toml
[vars]
TONBO_S3_ENDPOINT = "https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com"
TONBO_S3_BUCKET = "your-bucket-name"
TONBO_S3_REGION = "auto"
```

Your Account ID can be found in:
- R2 overview page URL: `dash.cloudflare.com/ACCOUNT_ID/r2/...`
- Or run: `npx wrangler whoami`

#### Set secrets

```bash
npx wrangler secret put TONBO_S3_ACCESS_KEY
# Paste your R2 Access Key ID when prompted

npx wrangler secret put TONBO_S3_SECRET_KEY
# Paste your R2 Secret Access Key when prompted
```

#### Deploy

```bash
npx wrangler deploy
```

#### Test the deployment

```bash
# Write data and read it back
curl -X POST https://your-worker.your-subdomain.workers.dev/write

# Expected output:
# Wrote 2 rows (alice=100, bob=200) to Tonbo DB.
# Read back: alice = 100
# Note: Cloudflare Workers have subrequest limits...
```

## How It Works

This example uses Tonbo with:

- **WebExecutor**: Fusio's executor for browser/WASM environments
- **AmazonS3**: S3-compatible filesystem backend (works with R2, LocalStack, MinIO, etc.)
- **Arrow RecordBatch**: For efficient columnar data storage

The worker demonstrates:
1. **POST /write** - Opens database, inserts 2 rows, reads one back (in same request)
2. **GET /read** - Opens database, queries specific keys
3. **GET /debug** - Lists files in S3/R2 bucket

## Important Limitations

### Cloudflare Workers Subrequest Limit

Cloudflare Workers limit HTTP subrequests per invocation. Tonbo operations (opening database, reading manifests, WAL replay, scanning data) each require HTTP requests to S3/R2.

| Plan | Subrequests | Tonbo Compatibility |
|------|-------------|---------------------|
| Free | 50 | Limited - write+read in same request |
| Paid | 1,000 | Good - separate operations should work |

**On the free tier (50 subrequests):**
- Write + single read works in one request ✓
- Multiple reads may exceed the limit ✗
- Separate read requests may fail if state accumulated ✗

**On the paid tier (1,000 subrequests):**
- Most Tonbo operations should work fine
- Separate read requests after writes should work
- Consider compaction to reduce files over time for best performance

**Workarounds for free tier:**
- Do write and immediate verification read in the same request
- Use Cloudflare Durable Objects for more complex operations
- Upgrade to paid plan for production use

## Size Optimization

Cloudflare Workers have a 10MB size limit. This example includes optimizations in `Cargo.toml`:

```toml
[profile.release]
opt-level = "z"       # Optimize for size
lto = "fat"           # Full link-time optimization
strip = "symbols"     # Strip debug symbols
codegen-units = 1     # Better optimization
panic = "abort"       # Smaller panic handling
```

Current build size: ~6MB (well under the limit).

## Troubleshooting

### "SignatureDoesNotMatch" errors

This usually means CORS isn't configured correctly, or the S3 endpoint isn't accessible from the worker. Ensure:
- CORS is configured on the bucket (see setup steps above)
- The endpoint URL is correct
- Credentials are set properly

### Build errors with fusio

This example requires a patched version of fusio with WASM fixes. See [fusio PR #257](https://github.com/tonbo-io/fusio/pull/257). The `[patch.crates-io]` section in `Cargo.toml` handles this.

### WAL persistence limitation

Currently, small files (like WAL segments < 5MB) are only persisted to S3 on `close()`, not on `flush()`. This means:
- Write + read in the **same request** works (data in memory)
- Write in one request, read in another may not see the data if `close()` wasn't called

This is a known limitation in fusio's S3Writer. A fix is needed to make `flush()` persist small files, but it requires careful handling of the append code path. See the fusio repository for updates.

### "error: executor-web is only supported on wasm32 targets"

This happens when running `cargo check` without the wasm32 target. Use:

```bash
cargo check --target wasm32-unknown-unknown
```

Or just use `npx wrangler build` which handles this automatically.

## Project Structure

```
examples/cloudflare-worker/
├── Cargo.toml      # Dependencies and build config
├── wrangler.toml   # Cloudflare Workers configuration
├── src/
│   └── lib.rs      # Worker implementation
├── .dev.vars       # Local development secrets (git-ignored)
└── .gitignore
```

## Learn More

- [Tonbo Documentation](https://docs.rs/tonbo)
- [Cloudflare Workers Documentation](https://developers.cloudflare.com/workers/)
- [Cloudflare R2 Documentation](https://developers.cloudflare.com/r2/)
- [Fusio Documentation](https://docs.rs/fusio)
