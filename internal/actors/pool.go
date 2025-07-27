package actors

import (
	"fmt"
	"github.com/anthdm/hollywood/actor"
	"hash/fnv"
)

type Pool struct {
	actors []*actor.PID
	size   int
}

func (p *Pool) GetActor(name string) *actor.PID {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(name))
	index := int(hash.Sum32()) % p.size
	return p.actors[index]
}

func NewPool(root *actor.Engine, props actor.Producer, kind string, poolSize, inboxSize int) *Pool {
	pool := &Pool{size: poolSize}

	for i := 0; i < poolSize; i++ {
		name := fmt.Sprintf("%s-actor-%d", kind, i)

		pid := root.Spawn(props, name, actor.WithInboxSize(inboxSize))

		pool.actors = append(pool.actors, pid)
	}

	return pool
}
