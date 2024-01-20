/*
 * Copyright (c) 2023 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package actorcomm

import (
	"sync"

	"github.com/asynkron/protoactor-go/actor"
)

var (
	once        sync.Once
	lock        sync.RWMutex
	ponce       sync.Once
	actorSystem *actor.ActorSystem
)

func InitActorSystem(aSystem *actor.ActorSystem) {
	once.Do(func() {
		actorSystem = aSystem
	})
}

// GetActorSystem must be executed before InitActorSystem, otherwise it returns nil
func GetActorSystem() *actor.ActorSystem {
	lock.RLock()
	defer lock.RUnlock()
	return actorSystem
}
