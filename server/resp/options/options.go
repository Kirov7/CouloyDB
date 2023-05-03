package options

import (
	"github.com/Kirov7/CouloyDB"
)

type KuloyOptions struct {
	StandaloneOpt CouloyDB.Options
	Peers         []string
	Self          int
}

func (ko *KuloyOptions) SetClusterPeers(self int, peers ...string) {
	ko.Self = self
	ko.Peers = peers
}

func (ko KuloyOptions) IsClusterOptions() bool {
	return len(ko.Peers) > 0
}
