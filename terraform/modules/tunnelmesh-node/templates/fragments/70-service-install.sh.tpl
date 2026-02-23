# Start the tunnelmesh service
%{ if coordinator_enabled ~}
systemctl start tunnelmesh-server
%{ else ~}
systemctl start tunnelmesh
%{ endif ~}
