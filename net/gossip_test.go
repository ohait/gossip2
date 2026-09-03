package gossip_test

import (
	"testing"

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

func TestNetReplay(t *testing.T) {
	impl, err := lib.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	addr := "127.0.0.1:12346"
	server := net.Server{Client: impl}
	if err := server.Listen(addr); err != nil {
		t.Fatal(err)
	}

	publisher, err := net.New(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.(interface{ Close() error }).Close()

	v, err := publisher.Publish("test", "FOO", 0, []byte("FOO"))
	if err != nil {
		t.Fatal(err)
	}
	if v == 0 {
		t.Fatal("expected non-zero version")
	}

	replayer, err := net.New(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer replayer.(interface{ Close() error }).Close()

	got := make(chan struct {
		topic string
		id    string
		v     lib.Version
		data  string
	}, 1)
	_, err = replayer.Subscribe("test", func(topic, id string, gotVersion lib.Version, data []byte) error {
		got <- struct {
			topic string
			id    string
			v     lib.Version
			data  string
		}{topic, id, gotVersion, string(data)}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-got:
		if got.topic != "test" || got.id != "FOO" || got.v != v || got.data != "FOO" {
			t.Fatalf("replay = %+v, want topic=test id=FOO v=%d data=FOO", got, v)
		}
	default:
		t.Fatal("expected FOO to be replayed")
	}
}

func testBasic(t *testing.T, cli lib.Client) {
	unsub, err := cli.Subscribe("test", func(topic, id string, v lib.Version, data []byte) error {
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
