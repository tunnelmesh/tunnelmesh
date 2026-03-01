package dns

import (
	"net"
	"strconv"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tunnelmesh/tunnelmesh/testutil"
)

func TestResolver_AddRecord(t *testing.T) {
	r := NewResolver(".tunnelmesh", 60)

	r.AddRecord("mynode", "10.42.0.1")
	r.AddRecord("other", "10.42.0.2")

	ip, ok := r.Resolve("mynode.tunnelmesh")
	assert.True(t, ok)
	assert.Equal(t, "10.42.0.1", ip)

	ip, ok = r.Resolve("other.tunnelmesh")
	assert.True(t, ok)
	assert.Equal(t, "10.42.0.2", ip)
}

func TestResolver_RemoveRecord(t *testing.T) {
	r := NewResolver(".tunnelmesh", 60)

	r.AddRecord("mynode", "10.42.0.1")

	ip, ok := r.Resolve("mynode.tunnelmesh")
	assert.True(t, ok)
	assert.Equal(t, "10.42.0.1", ip)

	r.RemoveRecord("mynode")

	_, ok = r.Resolve("mynode.tunnelmesh")
	assert.False(t, ok)
}

func TestResolver_UpdateRecords(t *testing.T) {
	r := NewResolver(".tunnelmesh", 60)

	// Initial records
	r.AddRecord("node1", "10.42.0.1")
	r.AddRecord("node2", "10.42.0.2")

	// Bulk update - should replace all
	r.UpdateRecords(map[string]string{
		"node3": "10.42.0.3",
		"node4": "10.42.0.4",
	})

	// Old records should be gone
	_, ok := r.Resolve("node1.tunnelmesh")
	assert.False(t, ok)
	_, ok = r.Resolve("node2.tunnelmesh")
	assert.False(t, ok)

	// New records should exist
	ip, ok := r.Resolve("node3.tunnelmesh")
	assert.True(t, ok)
	assert.Equal(t, "10.42.0.3", ip)
}

func TestResolver_ResolveWithoutSuffix(t *testing.T) {
	r := NewResolver(".tunnelmesh", 60)
	r.AddRecord("mynode", "10.42.0.1")

	// Should work with or without suffix
	ip, ok := r.Resolve("mynode.tunnelmesh")
	assert.True(t, ok)
	assert.Equal(t, "10.42.0.1", ip)

	ip, ok = r.Resolve("mynode")
	assert.True(t, ok)
	assert.Equal(t, "10.42.0.1", ip)
}

func TestResolver_ResolveNonexistent(t *testing.T) {
	r := NewResolver(".tunnelmesh", 60)

	_, ok := r.Resolve("unknown.tunnelmesh")
	assert.False(t, ok)
}

func TestResolver_DNSServer(t *testing.T) {
	port := testutil.FreePort(t)
	addr := "127.0.0.1:" + strconv.Itoa(port)

	r := NewResolver(".tunnelmesh", 60)
	r.AddRecord("testhost", "10.42.0.42")

	// Start server
	go func() {
		err := r.ListenAndServe(addr)
		if err != nil {
			t.Logf("DNS server error: %v", err)
		}
	}()
	<-r.Started()
	defer func() { _ = r.Shutdown() }()

	// Query the DNS server
	c := new(dns.Client)
	m := new(dns.Msg)
	m.SetQuestion("testhost.tunnelmesh.", dns.TypeA)

	resp, _, err := c.Exchange(m, addr)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Answer, 1)

	a, ok := resp.Answer[0].(*dns.A)
	require.True(t, ok)
	assert.Equal(t, net.ParseIP("10.42.0.42").To4(), a.A.To4())
}

func TestResolver_DNSServer_NXDOMAIN(t *testing.T) {
	port := testutil.FreePort(t)
	addr := "127.0.0.1:" + strconv.Itoa(port)

	r := NewResolver(".tunnelmesh", 60)

	go func() {
		_ = r.ListenAndServe(addr)
	}()
	<-r.Started()
	defer func() { _ = r.Shutdown() }()

	c := new(dns.Client)
	m := new(dns.Msg)
	m.SetQuestion("unknown.tunnelmesh.", dns.TypeA)

	resp, _, err := c.Exchange(m, addr)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, dns.RcodeNameError, resp.Rcode)
}

func TestResolver_SetCoordMeshIPs(t *testing.T) {
	r := NewResolver(".tunnelmesh", 60)

	// Initially "this" should not resolve
	_, ok := r.Resolve("this.tunnelmesh")
	assert.False(t, ok)
	ips, ok := r.ResolveAll("this.tunnelmesh")
	assert.False(t, ok)
	assert.Nil(t, ips)

	// Set single coordinator IP
	r.SetCoordMeshIPs([]string{"10.42.0.1"})
	ip, ok := r.Resolve("this.tunnelmesh")
	assert.True(t, ok)
	assert.Equal(t, "10.42.0.1", ip)
	ips, ok = r.ResolveAll("this.tunnelmesh")
	assert.True(t, ok)
	assert.Equal(t, []string{"10.42.0.1"}, ips)

	// Set multiple coordinator IPs for round-robin
	r.SetCoordMeshIPs([]string{"10.42.0.1", "10.42.0.2", "10.42.0.3"})
	ips, ok = r.ResolveAll("this.tunnelmesh")
	assert.True(t, ok)
	assert.Equal(t, []string{"10.42.0.1", "10.42.0.2", "10.42.0.3"}, ips)

	// Resolve() returns only first IP
	ip, ok = r.Resolve("this.tunnelmesh")
	assert.True(t, ok)
	assert.Equal(t, "10.42.0.1", ip)

	// Defensive copy: modifying the original slice should not affect the resolver
	original := []string{"10.42.0.10", "10.42.0.11"}
	r.SetCoordMeshIPs(original)
	original[0] = "MODIFIED"
	ips, ok = r.ResolveAll("this.tunnelmesh")
	assert.True(t, ok)
	assert.Equal(t, "10.42.0.10", ips[0])

	// Clear coordinator IPs
	r.SetCoordMeshIPs(nil)
	_, ok = r.Resolve("this.tunnelmesh")
	assert.False(t, ok)
}

func TestResolver_ResolveAll_RegularHostname(t *testing.T) {
	r := NewResolver(".tunnelmesh", 60)
	r.AddRecord("mynode", "10.42.0.5")

	ips, ok := r.ResolveAll("mynode.tunnelmesh")
	assert.True(t, ok)
	assert.Equal(t, []string{"10.42.0.5"}, ips)

	// Nonexistent
	ips, ok = r.ResolveAll("unknown.tunnelmesh")
	assert.False(t, ok)
	assert.Nil(t, ips)
}

func TestResolver_DNSServer_MultipleARecords(t *testing.T) {
	port := testutil.FreePort(t)
	addr := "127.0.0.1:" + strconv.Itoa(port)

	r := NewResolver(".tunnelmesh", 60)
	r.SetCoordMeshIPs([]string{"10.42.0.1", "10.42.0.2", "10.42.0.3"})

	go func() {
		_ = r.ListenAndServe(addr)
	}()
	<-r.Started()
	defer func() { _ = r.Shutdown() }()

	c := new(dns.Client)
	m := new(dns.Msg)
	m.SetQuestion("this.tunnelmesh.", dns.TypeA)

	resp, _, err := c.Exchange(m, addr)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Answer, 3)

	expectedIPs := []string{"10.42.0.1", "10.42.0.2", "10.42.0.3"}
	for i, ans := range resp.Answer {
		a, ok := ans.(*dns.A)
		require.True(t, ok)
		assert.Equal(t, net.ParseIP(expectedIPs[i]).To4(), a.A.To4())
	}
}

func TestResolver_DNSServer_AllCoordinatorsDeregistered(t *testing.T) {
	port := testutil.FreePort(t)
	addr := "127.0.0.1:" + strconv.Itoa(port)

	r := NewResolver(".tunnelmesh", 60)
	r.SetCoordMeshIPs([]string{"10.42.0.1"})

	go func() {
		_ = r.ListenAndServe(addr)
	}()
	<-r.Started()
	defer func() { _ = r.Shutdown() }()

	c := new(dns.Client)

	// Should resolve initially
	m := new(dns.Msg)
	m.SetQuestion("this.tm.", dns.TypeA)
	resp, _, err := c.Exchange(m, addr)
	require.NoError(t, err)
	assert.Len(t, resp.Answer, 1)

	// Clear all coordinator IPs (simulates all coordinators leaving)
	r.SetCoordMeshIPs(nil)

	m = new(dns.Msg)
	m.SetQuestion("this.tm.", dns.TypeA)
	resp, _, err = c.Exchange(m, addr)
	require.NoError(t, err)
	assert.Equal(t, dns.RcodeNameError, resp.Rcode)
	assert.Empty(t, resp.Answer)
}

func TestResolver_ListRecords(t *testing.T) {
	r := NewResolver(".tunnelmesh", 60)

	r.AddRecord("node1", "10.42.0.1")
	r.AddRecord("node2", "10.42.0.2")

	records := r.ListRecords()
	assert.Len(t, records, 2)
	assert.Equal(t, "10.42.0.1", records["node1"])
	assert.Equal(t, "10.42.0.2", records["node2"])
}
