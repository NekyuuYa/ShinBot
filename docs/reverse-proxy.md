# Reverse Proxy Setup for HTTPS

ShinBot's management API runs on plain HTTP by default. For production deployments, you should use a reverse proxy to handle HTTPS termination.

## Quick Start with Caddy (Recommended)

Caddy automatically handles HTTPS certificates.

```bash
# Install Caddy
# https://caddyserver.com/docs/install

# Create Caddyfile
cat > Caddyfile << 'EOF'
shinbot.example.com {
    reverse_proxy localhost:3945
}
EOF

# Start Caddy
caddy run
```

Caddy will automatically obtain and renew SSL certificates from Let's Encrypt.

## nginx Setup

### Install nginx

```bash
# Ubuntu/Debian
sudo apt install nginx

# CentOS/RHEL
sudo yum install nginx
```

### Configure nginx

Create `/etc/nginx/sites-available/shinbot`:

```nginx
server {
    listen 80;
    server_name shinbot.example.com;
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name shinbot.example.com;

    ssl_certificate /etc/letsencrypt/live/shinbot.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/shinbot.example.com/privkey.pem;

    # Modern SSL configuration
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384;
    ssl_prefer_server_ciphers off;

    location / {
        proxy_pass http://localhost:3945;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket support
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

### Enable the site

```bash
sudo ln -s /etc/nginx/sites-available/shinbot /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

## Apache Setup

### Install Apache

```bash
# Ubuntu/Debian
sudo apt install apache2

# Enable required modules
sudo a2enmod ssl proxy proxy_http proxy_wstunnel rewrite
```

### Configure Apache

Create `/etc/apache2/sites-available/shinbot.conf`:

```apache
<VirtualHost *:80>
    ServerName shinbot.example.com
    Redirect permanent / https://shinbot.example.com/
</VirtualHost>

<VirtualHost *:443>
    ServerName shinbot.example.com

    SSLEngine on
    SSLCertificateFile /etc/letsencrypt/live/shinbot.example.com/fullchain.pem
    SSLCertificateKeyFile /etc/letsencrypt/live/shinbot.example.com/privkey.pem

    ProxyPreserveHost On
    ProxyPass / http://localhost:3945/
    ProxyPassReverse / http://localhost:3945/

    # WebSocket support
    RewriteEngine On
    RewriteCond %{HTTP:Upgrade} =websocket [NC]
    RewriteRule /(.*) ws://localhost:3945/$1 [P,L]
</VirtualHost>
```

### Enable the site

```bash
sudo a2ensite shinbot.conf
sudo systemctl reload apache2
```

## Built-in SSL Support

ShinBot also supports built-in SSL without a reverse proxy. This is useful for development or simple deployments.

### Using CLI arguments

```bash
# Generate self-signed certificate (for testing)
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes

# Start with SSL
uv run main.py --ssl-certfile cert.pem --ssl-keyfile key.pem
```

### Using config file

Add to `data/config.toml`:

```toml
[admin]
ssl_certfile = "/path/to/cert.pem"
ssl_keyfile = "/path/to/key.pem"
# ssl_keyfile_password = "optional-password"
```

## Security Recommendations

1. **Use Let's Encrypt** for production certificates (free and automatic)
2. **Enable HSTS** in your reverse proxy configuration
3. **Restrict CORS origins** in ShinBot config (don't use `["*"]`)
4. **Use strong passwords** for admin credentials
5. **Keep ShinBot updated** to receive security patches
