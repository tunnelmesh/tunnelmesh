package coord

import (
	"net/http"
	"net/http/httputil"
	"net/url"
)

// MonitoringProxyConfig holds configuration for reverse proxying to monitoring services.
type MonitoringProxyConfig struct {
	PrometheusURL string // e.g., "http://localhost:9090"
	GrafanaURL    string // e.g., "http://localhost:3000"
}

// SetupMonitoringProxies registers reverse proxy handlers for Prometheus and Grafana.
// These are registered on the adminMux for access via https://this.tunnelmesh/
// If a service URL is configured locally, traffic is proxied to localhost.
// Otherwise, traffic is forwarded to a coordinator that has monitoring configured.
// Prometheus should be configured with --web.route-prefix=/prometheus/
// Grafana should be configured with GF_SERVER_SERVE_FROM_SUB_PATH=true
func (s *Server) SetupMonitoringProxies(cfg MonitoringProxyConfig) {
	if s.adminMux == nil {
		return
	}

	// Prometheus handler
	if cfg.PrometheusURL != "" {
		promURL, err := url.Parse(cfg.PrometheusURL)
		if err == nil {
			proxy := &httputil.ReverseProxy{
				Director: func(req *http.Request) {
					req.URL.Scheme = promURL.Scheme
					req.URL.Host = promURL.Host
					req.Host = promURL.Host
				},
			}
			s.adminMux.HandleFunc("/prometheus/", func(w http.ResponseWriter, r *http.Request) {
				proxy.ServeHTTP(w, r)
			})
		}
	} else {
		s.adminMux.HandleFunc("/prometheus/", func(w http.ResponseWriter, r *http.Request) {
			s.forwardToMonitoringCoordinator(w, r)
		})
	}

	// Grafana handler
	if cfg.GrafanaURL != "" {
		grafanaURL, err := url.Parse(cfg.GrafanaURL)
		if err == nil {
			proxy := &httputil.ReverseProxy{
				Director: func(req *http.Request) {
					req.URL.Scheme = grafanaURL.Scheme
					req.URL.Host = grafanaURL.Host
					req.Host = grafanaURL.Host
				},
			}
			s.adminMux.HandleFunc("/grafana/", func(w http.ResponseWriter, r *http.Request) {
				proxy.ServeHTTP(w, r)
			})
		}
	} else {
		s.adminMux.HandleFunc("/grafana/", func(w http.ResponseWriter, r *http.Request) {
			s.forwardToMonitoringCoordinator(w, r)
		})
	}
}

// forwardToMonitoringCoordinator proxies the request to a coordinator that has monitoring configured.
func (s *Server) forwardToMonitoringCoordinator(w http.ResponseWriter, r *http.Request) {
	// Detect forwarding loops — if another coordinator already forwarded this request, stop.
	if r.Header.Get("X-TunnelMesh-Forwarded") != "" {
		http.Error(w, "monitoring proxy forwarding loop detected", http.StatusLoopDetected)
		return
	}

	s.peersMu.RLock()
	var targetIP string
	for _, ci := range s.coordinators {
		if ci.hasMonitoring {
			targetIP = ci.peer.MeshIP
			break
		}
	}
	s.peersMu.RUnlock()

	if targetIP == "" {
		http.Error(w, "no monitoring coordinator available", http.StatusServiceUnavailable)
		return
	}

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "https"
			req.URL.Host = targetIP
			req.Host = targetIP
			req.Header.Set("X-TunnelMesh-Forwarded", "true")
		},
		// Use shared transport to reuse connections and avoid goroutine leaks.
		// Initialized in NewServer; falls back to default transport if nil (e.g. bare test structs).
		Transport: s.monitoringTransport,
	}
	proxy.ServeHTTP(w, r)
}
