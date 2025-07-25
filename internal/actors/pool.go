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

func NewPool(root *actor.Engine, props actor.Producer, size int) *Pool {
	pool := &Pool{size: size}

	for i := 0; i < size; i++ {
		name := fmt.Sprintf("actor-%d", i)

		pid := root.Spawn(props, name)

		pool.actors = append(pool.actors, pid)
	}

	return pool
}
