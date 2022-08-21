package migrate

import (
	"strings"

	builtin0 "github.com/filecoin-project/specs-actors/actors/builtin"
	builtin2 "github.com/filecoin-project/specs-actors/v2/actors/builtin"
	builtin3 "github.com/filecoin-project/specs-actors/v3/actors/builtin"
	builtin4 "github.com/filecoin-project/specs-actors/v4/actors/builtin"
	builtin5 "github.com/filecoin-project/specs-actors/v5/actors/builtin"
	builtin6 "github.com/filecoin-project/specs-actors/v6/actors/builtin"
	builtin7 "github.com/filecoin-project/specs-actors/v7/actors/builtin"
	"github.com/ipfs/go-cid"
)

func ActorNameByCode(c cid.Cid) string {
	switch {

	case builtin0.IsBuiltinActor(c):
		return builtin0.ActorNameByCode(c)

	case builtin2.IsBuiltinActor(c):
		return builtin2.ActorNameByCode(c)

	case builtin3.IsBuiltinActor(c):
		return builtin3.ActorNameByCode(c)

	case builtin4.IsBuiltinActor(c):
		return builtin4.ActorNameByCode(c)

	case builtin5.IsBuiltinActor(c):
		return builtin5.ActorNameByCode(c)

	case builtin6.IsBuiltinActor(c):
		return builtin6.ActorNameByCode(c)

	case builtin7.IsBuiltinActor(c):
		return builtin7.ActorNameByCode(c)

	default:
		return "<unknown>"
	}
}

func ActorFamily(name string) string {
	if name == "<unknown>" {
		return "<unknown>"
	}

	if !strings.HasPrefix(name, "fil/") {
		return "<unknown>"
	}
	idx := strings.LastIndex(name, "/")
	if idx == -1 {
		return "<unknown>"
	}

	return name[idx+1:]
}
