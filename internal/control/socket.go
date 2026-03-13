// Package control provides a Unix socket server for CLI-to-daemon communication.
package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tunnelmesh/tunnelmesh/internal/routing"
)

// DefaultSocketPath returns the default control socket path.
func DefaultSocketPath() string {
	return "/var/run/tunnelmesh.sock"
}

// Request types for control commands.
const (
	CmdFilterList   = "filter.list"
	CmdFilterAdd    = "filter.add"
	CmdFilterRemove = "filter.remove"
)

// Timeouts for control socket operations.
const (
	// SocketDialTimeout is the timeout for connecting to the control socket.
	SocketDialTimeout = 5 * time.Second
	// SocketReadWriteTimeout is the timeout for reading/writing on the socket.
	SocketReadWriteTimeout = 5 * time.Second
)

// Request is a control command from the CLI.
type Request struct {
	Command string          `json:"command"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Response is a response to a control command.
type Response struct {
	Success bool            `json:"success"`
	Error   string          `json:"error,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// FilterAddRequest is the payload for filter.add command.
type FilterAddRequest struct {
	Port       uint16 `json:"port"`
	Protocol   string `json:"protocol"`    // "tcp" or "udp"
	Action     string `json:"action"`      // "allow" or "deny"
	TTL        int64  `json:"ttl"`         // Seconds until expiry, 0 = permanent
	SourcePeer string `json:"source_peer"` // Source peer name (optional, empty = any)
}

// FilterRemoveRequest is the payload for filter.remove command.
type FilterRemoveRequest struct {
	Port       uint16 `json:"port"`
	Protocol   string `json:"protocol"`
	SourcePeer string `json:"source_peer"` // Source peer name (optional, empty = global rule)
}

// FilterListResponse is the response for filter.list command.
type FilterListResponse struct {
	DefaultDeny bool               `json:"default_deny"`
	Rules       []FilterRuleDetail `json:"rules"`
}

// FilterRuleDetail includes rule info and source for display.
type FilterRuleDetail struct {
	Port       uint16 `json:"port"`
	Protocol   string `json:"protocol"`
	Action     string `json:"action"`
	Source     string `json:"source"`      // "coordinator", "config", "temporary"
	Expires    int64  `json:"expires"`     // Unix timestamp, 0 = permanent
	SourcePeer string `json:"source_peer"` // Source peer name (empty = any peer)
}

// Server is a Unix socket control server.
type Server struct {
	socketPath      string
	filter          *routing.PacketFilter
	localPeerName   string
	listener        net.Listener
	onFilterChanged func() // Called after filter changes (for persistence)
	mu              sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewServer creates a new control server.
// localPeerName is the name of the local peer, used for validation.
func NewServer(socketPath string, filter *routing.PacketFilter, localPeerName string) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		socketPath:    socketPath,
		filter:        filter,
		localPeerName: localPeerName,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// SetFilter updates the packet filter reference.
func (s *Server) SetFilter(filter *routing.PacketFilter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.filter = filter
}

// SetFilterChangedHandler sets the callback for filter changes.
func (s *Server) SetFilterChangedHandler(handler func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onFilterChanged = handler
}

// Start begins listening on the control socket.
func (s *Server) Start() error {
	// Ensure parent directory exists
	dir := filepath.Dir(s.socketPath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create socket directory: %w", err)
	}

	// Remove stale socket
	if err := os.Remove(s.socketPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale socket: %w", err)
	}

	listener, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return fmt.Errorf("listen on socket: %w", err)
	}

	// Restrict socket permissions
	if err := os.Chmod(s.socketPath, 0600); err != nil {
		_ = listener.Close()
		return fmt.Errorf("chmod socket: %w", err)
	}

	s.listener = listener
	log.Info().Str("path", s.socketPath).Msg("control socket listening")

	go s.acceptLoop()
	return nil
}

// Stop closes the control server.
func (s *Server) Stop() error {
	s.cancel()
	if s.listener != nil {
		_ = s.listener.Close()
	}
	_ = os.Remove(s.socketPath)
	return nil
}

// SocketPath returns the socket path.
func (s *Server) SocketPath() string {
	return s.socketPath
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				log.Error().Err(err).Msg("control socket accept error")
				continue
			}
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	// Set read deadline
	_ = conn.SetDeadline(time.Now().Add(SocketReadWriteTimeout))

	// Read request
	decoder := json.NewDecoder(conn)
	var req Request
	if err := decoder.Decode(&req); err != nil {
		s.sendError(conn, fmt.Errorf("decode request: %w", err))
		return
	}

	// Handle command
	resp := s.handleCommand(req)

	// Send response
	encoder := json.NewEncoder(conn)
	_ = encoder.Encode(resp)
}

func (s *Server) handleCommand(req Request) Response {
	s.mu.RLock()
	filter := s.filter
	s.mu.RUnlock()

	if filter == nil {
		return Response{Success: false, Error: "packet filter not initialized"}
	}

	switch req.Command {
	case CmdFilterList:
		return s.handleFilterList(filter)
	case CmdFilterAdd:
		return s.handleFilterAdd(filter, req.Payload)
	case CmdFilterRemove:
		return s.handleFilterRemove(filter, req.Payload)
	default:
		return Response{Success: false, Error: fmt.Sprintf("unknown command: %s", req.Command)}
	}
}

func (s *Server) handleFilterList(filter *routing.PacketFilter) Response {
	rules := filter.ListRules()

	details := make([]FilterRuleDetail, 0, len(rules))
	for _, r := range rules {
		details = append(details, FilterRuleDetail{
			Port:       r.Rule.Port,
			Protocol:   routing.ProtocolToString(r.Rule.Protocol),
			Action:     r.Rule.Action.String(),
			Source:     r.Source.String(),
			Expires:    r.Rule.Expires,
			SourcePeer: r.Rule.SourcePeer,
		})
	}

	resp := FilterListResponse{
		DefaultDeny: filter.IsDefaultDeny(),
		Rules:       details,
	}

	data, _ := json.Marshal(resp)
	return Response{Success: true, Data: data}
}

func (s *Server) handleFilterAdd(filter *routing.PacketFilter, payload json.RawMessage) Response {
	var req FilterAddRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return Response{Success: false, Error: fmt.Sprintf("invalid payload: %v", err)}
	}

	// Validate
	if req.Port == 0 {
		return Response{Success: false, Error: "invalid port: must be between 1 and 65535 (port is required)"}
	}
	proto := routing.ProtocolFromString(req.Protocol)
	if proto == 0 {
		return Response{Success: false, Error: fmt.Sprintf("invalid protocol %q - must be one of: tcp, udp", req.Protocol)}
	}
	action := routing.ParseFilterAction(req.Action)

	// Prevent self-targeting: a peer can't filter traffic from itself
	if req.SourcePeer != "" && req.SourcePeer == s.localPeerName {
		return Response{Success: false, Error: "a peer cannot filter traffic from itself"}
	}

	// Calculate expiry
	var expires int64
	if req.TTL > 0 {
		expires = time.Now().Unix() + req.TTL
	}

	rule := routing.FilterRule{
		Port:       req.Port,
		Protocol:   proto,
		Action:     action,
		Expires:    expires,
		SourcePeer: req.SourcePeer,
	}

	filter.AddTemporaryRule(rule)

	// Notify persistence layer
	s.mu.RLock()
	handler := s.onFilterChanged
	s.mu.RUnlock()
	if handler != nil {
		handler()
	}

	log.Info().
		Uint16("port", req.Port).
		Str("protocol", req.Protocol).
		Str("action", req.Action).
		Str("source_peer", req.SourcePeer).
		Int64("ttl", req.TTL).
		Msg("added temporary filter rule via control socket")

	return Response{Success: true}
}

func (s *Server) handleFilterRemove(filter *routing.PacketFilter, payload json.RawMessage) Response {
	var req FilterRemoveRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return Response{Success: false, Error: fmt.Sprintf("invalid payload: %v", err)}
	}

	proto := routing.ProtocolFromString(req.Protocol)
	if proto == 0 {
		return Response{Success: false, Error: fmt.Sprintf("invalid protocol %q - must be one of: tcp, udp", req.Protocol)}
	}

	filter.RemoveTemporaryRuleForPeer(req.Port, proto, req.SourcePeer)

	// Notify persistence layer
	s.mu.RLock()
	handler := s.onFilterChanged
	s.mu.RUnlock()
	if handler != nil {
		handler()
	}

	log.Info().
		Uint16("port", req.Port).
		Str("protocol", req.Protocol).
		Str("source_peer", req.SourcePeer).
		Msg("removed temporary filter rule via control socket")

	return Response{Success: true}
}

func (s *Server) sendError(conn net.Conn, err error) {
	resp := Response{Success: false, Error: err.Error()}
	_ = json.NewEncoder(conn).Encode(resp)
}

// Client is a control socket client for CLI commands.
type Client struct {
	socketPath string
}

// NewClient creates a new control client.
func NewClient(socketPath string) *Client {
	return &Client{socketPath: socketPath}
}

// Send sends a request and returns the response.
func (c *Client) Send(req Request) (*Response, error) {
	conn, err := net.DialTimeout("unix", c.socketPath, SocketDialTimeout)
	if err != nil {
		return nil, fmt.Errorf("connect to control socket: %w", err)
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(SocketReadWriteTimeout))

	// Send request
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}

	// Read response
	var resp Response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	return &resp, nil
}

// FilterList retrieves the current filter rules.
func (c *Client) FilterList() (*FilterListResponse, error) {
	resp, err := c.Send(Request{Command: CmdFilterList})
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, errors.New(resp.Error)
	}

	var result FilterListResponse
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	return &result, nil
}

// FilterAdd adds a temporary global filter rule.
func (c *Client) FilterAdd(port uint16, protocol, action string, ttl int64) error {
	return c.FilterAddForPeer(port, protocol, action, ttl, "")
}

// FilterAddForPeer adds a temporary filter rule for a specific source peer.
// Pass empty string for sourcePeer to create a global rule (applies to all peers).
func (c *Client) FilterAddForPeer(port uint16, protocol, action string, ttl int64, sourcePeer string) error {
	payload, _ := json.Marshal(FilterAddRequest{
		Port:       port,
		Protocol:   protocol,
		Action:     action,
		TTL:        ttl,
		SourcePeer: sourcePeer,
	})

	resp, err := c.Send(Request{Command: CmdFilterAdd, Payload: payload})
	if err != nil {
		return err
	}
	if !resp.Success {
		return errors.New(resp.Error)
	}
	return nil
}

// FilterRemove removes a temporary global filter rule.
func (c *Client) FilterRemove(port uint16, protocol string) error {
	return c.FilterRemoveForPeer(port, protocol, "")
}

// FilterRemoveForPeer removes a temporary filter rule for a specific source peer.
// Pass empty string for sourcePeer to remove a global rule.
func (c *Client) FilterRemoveForPeer(port uint16, protocol, sourcePeer string) error {
	payload, _ := json.Marshal(FilterRemoveRequest{
		Port:       port,
		Protocol:   protocol,
		SourcePeer: sourcePeer,
	})

	resp, err := c.Send(Request{Command: CmdFilterRemove, Payload: payload})
	if err != nil {
		return err
	}
	if !resp.Success {
		return errors.New(resp.Error)
	}
	return nil
}
