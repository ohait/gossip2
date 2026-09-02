package gossip_test

import (
	"testing"

	"github.com/ohait/gossip2/enc"
	lib "github.com/ohait/gossip2/lib"
	net "github.com/ohait/gossip2/net"
)

func TestLocal(t *testing.T) {
	dir := t.TempDir()
	cli, err := lib.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	testBasic(t, cli)
}

func TestNet(t *testing.T) {
	net.DBG = t.Logf
	dir := t.TempDir()
	impl, err := lib.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	netSrv := net.Server{Client: impl}
	netSrv.Listen("0.0.0.0:12345")

	netCli, err := net.New("0.0.0.0:12345")
	if err != nil {
		t.Fatal(err)
	}
	testBasic(t, netCli)
}

func testBasic(t *testing.T, cli lib.Client) {
	unsub, err := cli.Subscribe("test", func(topic, id string, v enc.Version, data []byte) error {
		t.Logf("received: topic=%s, id=%s, v=%d, data=%s", topic, id, v, data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer unsub()

	t.Logf("publishing alice@0")
	v1, err := cli.Publish("test", "alice", 0, []byte("Alice Join"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("published: v=%v", v1)
	if v1 == 0 {
		t.Fatal("expected non-zero version")
	}

	t.Logf("publishing alice@0 -> cas error")
	_, err = cli.Publish("test", "alice", 0, []byte("Bob Join"))
	if err == nil {
		t.Fatal("expected CAS error")
	}

	t.Logf("publishing alice@%v", v1)
	v3, err := cli.Publish("test", "alice", v1, []byte("Alice Updated"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("published: v=%v", v3)
	if v3 == 0 {
		t.Fatal("expected non-zero version")
	}
}
