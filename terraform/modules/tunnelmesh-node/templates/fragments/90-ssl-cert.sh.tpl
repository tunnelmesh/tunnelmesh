%{ if coordinator_enabled && ssl_enabled ~}
# Wait for DNS propagation and obtain SSL certificate
echo "Waiting for DNS propagation before SSL cert request..."
# Poll until the A record resolves to the reserved IP (not the droplet's own IP), or give up after 5 min
MY_IP="${reserved_ip}"
for i in $(seq 1 30); do
    RESOLVED=$(dig +short A ${node_name}.${domain} @8.8.8.8 | tail -1)
    if [ "$RESOLVED" = "$MY_IP" ]; then
        echo "DNS propagated: ${node_name}.${domain} -> $MY_IP"
        break
    fi
    echo "Waiting for DNS... attempt $i/30 (got '$RESOLVED', want '$MY_IP')"
    sleep 10
done

# Try to get certificate
certbot certonly --webroot -w /var/www/html \
    -d ${node_name}.${domain} \
    --non-interactive --agree-tos \
    --email ${ssl_email} || {
    echo "Certbot failed, will retry on next boot or manually"
    exit 0
}

# Switch to full SSL config
ln -sf /etc/nginx/sites-available/tunnelmesh /etc/nginx/sites-enabled/tunnelmesh
systemctl reload nginx

echo "SSL certificate installed successfully"
%{ endif ~}
