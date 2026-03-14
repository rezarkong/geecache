package algo

import "container/list"

const (
	arcT1 = iota + 1
	arcT2
	arcB1
	arcB2
)

type arcNode struct {
	key     string
	listID  int
	element *list.Element
}

// ARC approximates Adaptive Replacement Cache using live lists (T1/T2)
// plus ghost lists (B1/B2). Cache owns the actual values; ARC only tracks keys.
type ARC struct {
	p     int
	nodes map[string]*arcNode
	t1    *list.List
	t2    *list.List
	b1    *list.List
	b2    *list.List
}

func NewARC() *ARC {
	return &ARC{
		nodes: make(map[string]*arcNode),
		t1:    list.New(),
		t2:    list.New(),
		b1:    list.New(),
		b2:    list.New(),
	}
}

func (arc *ARC) OnAdd(key string) {
	if node, ok := arc.nodes[key]; ok {
		switch node.listID {
		case arcT1, arcT2:
			arc.OnAccess(key)
			return
		case arcB1:
			arc.p = minInt(arc.capacity(), arc.p+arc.adaptDelta(arc.b1.Len(), arc.b2.Len()))
			arc.move(node, arc.b1, arc.t2, arcT2)
			arc.trimGhosts()
			return
		case arcB2:
			arc.p = maxInt(0, arc.p-arc.adaptDelta(arc.b2.Len(), arc.b1.Len()))
			arc.move(node, arc.b2, arc.t2, arcT2)
			arc.trimGhosts()
			return
		}
	}

	node := &arcNode{key: key}
	arc.nodes[key] = node
	arc.pushFront(node, arc.t1, arcT1)
	arc.trimGhosts()
}

func (arc *ARC) OnAccess(key string) {
	node, ok := arc.nodes[key]
	if !ok {
		return
	}

	switch node.listID {
	case arcT1:
		arc.move(node, arc.t1, arc.t2, arcT2)
	case arcT2:
		arc.t2.MoveToFront(node.element)
	}
}

func (arc *ARC) OnRemove(key string) {
	node, ok := arc.nodes[key]
	if !ok {
		return
	}
	arc.removeNode(node)
	delete(arc.nodes, key)
	arc.clampP()
}

func (arc *ARC) OnEvict(key string) {
	node, ok := arc.nodes[key]
	if !ok {
		return
	}

	switch node.listID {
	case arcT1:
		arc.move(node, arc.t1, arc.b1, arcB1)
	case arcT2:
		arc.move(node, arc.t2, arc.b2, arcB2)
	default:
		return
	}
	arc.clampP()
	arc.trimGhosts()
}

func (arc *ARC) Evict() string {
	if arc.t1.Len() > 0 && (arc.t1.Len() > arc.p || (arc.t1.Len() == arc.p && arc.b2.Len() > 0)) {
		if ele := arc.t1.Back(); ele != nil {
			return ele.Value.(*arcNode).key
		}
	}
	if ele := arc.t2.Back(); ele != nil {
		return ele.Value.(*arcNode).key
	}
	if ele := arc.t1.Back(); ele != nil {
		return ele.Value.(*arcNode).key
	}
	return ""
}

func (arc *ARC) adaptDelta(primaryLen, secondaryLen int) int {
	if primaryLen == 0 {
		return 1
	}
	if secondaryLen > primaryLen {
		return secondaryLen / primaryLen
	}
	return 1
}

func (arc *ARC) capacity() int {
	capacity := arc.t1.Len() + arc.t2.Len()
	if capacity <= 0 {
		return 1
	}
	return capacity
}

func (arc *ARC) clampP() {
	arc.p = minInt(arc.p, arc.capacity())
	if arc.p < 0 {
		arc.p = 0
	}
}

func (arc *ARC) trimGhosts() {
	limit := arc.capacity()
	for arc.b1.Len()+arc.b2.Len() > limit {
		switch {
		case arc.b1.Len() > arc.b2.Len():
			arc.dropGhost(arc.b1)
		case arc.b2.Len() > 0:
			arc.dropGhost(arc.b2)
		default:
			arc.dropGhost(arc.b1)
		}
	}
}

func (arc *ARC) dropGhost(q *list.List) {
	ele := q.Back()
	if ele == nil {
		return
	}
	node := ele.Value.(*arcNode)
	q.Remove(ele)
	delete(arc.nodes, node.key)
}

func (arc *ARC) move(node *arcNode, from, to *list.List, listID int) {
	from.Remove(node.element)
	arc.pushFront(node, to, listID)
}

func (arc *ARC) pushFront(node *arcNode, q *list.List, listID int) {
	node.listID = listID
	node.element = q.PushFront(node)
}

func (arc *ARC) removeNode(node *arcNode) {
	switch node.listID {
	case arcT1:
		arc.t1.Remove(node.element)
	case arcT2:
		arc.t2.Remove(node.element)
	case arcB1:
		arc.b1.Remove(node.element)
	case arcB2:
		arc.b2.Remove(node.element)
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
