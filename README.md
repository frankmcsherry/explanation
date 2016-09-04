# explanation

Infrastructure for explaining the outputs of differential dataflow computations

## Explaining the outputs of data-parallel computations

This project demonstrates how one can use differential dataflow to explain the outputs of differential dataflow computations, as described in the paper [Explaining outputs in modern data analytics](http://www.vldb.org/pvldb/vol9/p1137-chothia.pdf). There is also an earlier [blog post](https://github.com/frankmcsherry/blog/blob/master/posts/2016-03-27.md) discussing the material, and trying to explain how it works.

In our context, programs run on input collections and produce output collections. An "explanation" for a subset of the output is a subset of the input so that if you ran the computation on this subset, it would produce at least the output subset you asked for. 

Concise explanations of this form can be super helpful, both for debugging your program and for understanding / communicating why parts of the output are the way they are. 

## A quick taste

To give a taste, we can explain the outputs of "prioritized label propagation", an algorithm for determining connected components in a graph, by tracking down which graph edges and labels were important to arrive at each of the individual output records, even as the graph changes. Here we load the `livejournal` graph dataset, asks for an explanation of node `123456`'s label, and then removes one of the edges on the explanatory path, causing the explanation to change:

	Echidnatron% cargo run --release --example interactive-cc -- ~/Projects/Datasets/livejournal
	     Running `target/release/examples/interactive-cc /Users/mcsherry/Projects/Datasets/livejournal`
	initialization elapsed:	Duration { secs: 55, nanos: 237042089 }

	> query + 123456 0
	label_must:	((0, 0), 1)
	graph_must:	((123456, 133128), 1)
	graph_must:	((0, 19), 1)
	graph_must:	((19, 133128), 1)
	round 1 elapsed:	Duration { secs: 0, nanos: 44231288 }

	> graph - 123456 133128
	graph_must:	((123456, 133128), -1)
	graph_must:	((73495, 123456), 1)
	graph_must:	((64, 47681), 1)
	graph_must:	((47681, 73495), 1)
	graph_must:	((0, 64), 1)
	round 2 elapsed:	Duration { secs: 0, nanos: 922563085 }

For the present the project is a library and some examples, demonstrating how to explain outputs rather than automatically doing so for all computations. The process does seem automatable, but the host frameworks don't lend themselves to easily *re-writing* dataflow computations.

## An example: connected components

In the `examples` directory you can find the `interactive-cc` example. This runs a connected components computation, via prioritized label propagation. The inputs are graph edges and node labels, and the outputs are labels assigned to nodes after propagation.

If you run the with no further arguments, it allows you to interactively add and remove graph edges, node labels, and to ask queries of the output labels. For example,

	Echidnatron% cargo run --example interactive-cc
	     Running `target/debug/examples/interactive-cc`
	initialization elapsed:	Duration { secs: 0, nanos: 856767 }
	> graph + 0 1
	round 1 elapsed:	Duration { secs: 0, nanos: 6668874 }
	> graph + 1 2
	round 2 elapsed:	Duration { secs: 0, nanos: 6224347 }
	> label + 0 0
	round 3 elapsed:	Duration { secs: 0, nanos: 21278949 }
	> query + 2 0
	label_must:	((0, 0), 1)
	graph_must:	((0, 1), 1)
	graph_must:	((1, 2), 1)
	round 4 elapsed:	Duration { secs: 0, nanos: 151156905 }

This is ... neat! We added some edges, from `0` to `1` to `2`, then added a label at `0`, and then asked about the label reaching `2`. The infrastructure tells us that to explain node `2` receiving label `0`, we need the label for `0` and the two edges. This probably isn't a huge surprise, so let's do a bit more.

	> graph + 1 3
	round 5 elapsed:	Duration { secs: 0, nanos: 13959840 }
	> query + 3 0
	graph_must:	((1, 3), 1)
	round 6 elapsed:	Duration { secs: 0, nanos: 77679489 }

This is similar. We've added an edge to node `3` and asked for an explanation. Or, more precisely, we have *added* `3` to the nodes who require explanation. Node `2` is still there, so we only get told about adding the edge `(1,3)` to the required input, as this is all we need to add to now explain the labels of both `2` and `3`. 

	> graph + 2 3
	round 7 elapsed:	Duration { secs: 0, nanos: 8583967 }

Nothing interesting here. We've added an edge from `2` to `3`, but it isn't used as part of an explanation, yet.

	> graph - 1 2
	graph_must:	((2, 3), 1)
	graph_must:	((1, 2), -1)
	round 8 elapsed:	Duration { secs: 0, nanos: 87361952 }

Whoa, we got rid of the edge from `1` to `2` which was crucial for explaining why `2` gets label `0`. It still does get label `0`, but now we need to bring in that edge between `2` and `3`.

### Larger graph data

If you go and snag the [livejournal graph dataset]() and wrap it up in the [graphmap format](), you can run the computation with an optional argument that loads a bunch of graph data for you. I'm also running it in `--release` mode now because otherwise it would be very slow.

	Echidnatron% cargo run --release --example interactive-cc -- ~/Projects/Datasets/livejournal
	     Running `target/release/examples/interactive-cc /Users/mcsherry/Projects/Datasets/livejournal`
	initialization elapsed:	Duration { secs: 55, nanos: 237042089 }

We can now interact with this, asking questions of a much larger computation: 

	> query + 123456 0
	label_must:	((0, 0), 1)
	graph_must:	((123456, 133128), 1)
	graph_must:	((0, 19), 1)
	graph_must:	((19, 133128), 1)
	round 1 elapsed:	Duration { secs: 0, nanos: 44231288 }

Here we have asked the computation to explain the label `0` for node `123456`. The "explanation" is given by the indicated input data. The label `(0,0)` propagates over edges `(0, 19)`, then `(19, 133128)`, then `(123456, 133128)`. The computation does not necessarily *need* these edges, as there may be other ways for `0` to reach `123456`, but these were the edges observed to provide that label when the computation executed.

Let's make life a bit more difficult, removing that last edge from the reported path and requiring a second path from `0` to `123456`, assuming they are still connected.

	> graph - 123456 133128
	graph_must:	((123456, 133128), -1)
	graph_must:	((73495, 123456), 1)
	graph_must:	((64, 47681), 1)
	graph_must:	((47681, 73495), 1)
	graph_must:	((0, 64), 1)
	round 2 elapsed:	Duration { secs: 0, nanos: 922563085 }

We see that the set of required edges changes; the edge `(123456, 133128)` no longer exists and is removed. Several new edges are added which reveal another path from `0` to `123456`.

One may (and should) wonder why the other explanatory edges along the shorter path are not removed. The reason is that explanations are currently for the full *history* of the queried node. It is reasonable to want only the current explanation, and it seems likely that the same type of techniques could provide this behavior as well. At the same time, this introduces some ambiguities that we don't yet know how to resolve generally.

The amount of time taken, 923ms, doesn't seem so great. It turns out this is just timely dataflow shaking out some dust when we first perform updates to the graph (and should probably be fixed). Now that it is sorted out, we can repeat the type of update and see more brisk responses.

Let's remove that edge to `123456` again.

	> graph - 73495 123456
	graph_must:	((73495, 123456), -1)
	graph_must:	((118715, 123456), 1)
	graph_must:	((64, 13112), 1)
	graph_must:	((13112, 118715), 1)
	round 3 elapsed:	Duration { secs: 0, nanos: 12515838 }

We see here again that removing the edge removes it from the explanations, and then another path is identified. This path appears shorter than above, but only because it has the ability to start at `64`, which is already required to explain the history of the label for `123456`.

The time to perform this update is just 13ms, which is relatively interactive.

What about a more aggressive change? Let's snip the edge just after `64`, cutting off more of the path

	> graph - 64 13112
	graph_must:	((64, 13112), -1)
	graph_must:	((117977, 118715), 1)
	graph_must:	((64, 117977), 1)
	round 4 elapsed:	Duration { secs: 0, nanos: 38432881 }

Here the explanation routes around the damage, going through `117977` rather than `13112` now that the latter is unavailable. This decision is made because when the computation runs without the edge `(64, 13112)` the zero label now arrives first through node `117977`. 

How about a "catastrophic" change: cutting the link to zero itself!

	> graph - 0 64
	graph_must:	((0, 64), -1)
	graph_must:	((0, 81), 1)
	graph_must:	((81, 117977), 1)
	round 5 elapsed:	Duration { secs: 0, nanos: 852422661 }

This is also a relatively simple change in explanation, but a substantially longer time to update: 852ms. The reason is that there is now much more work to do updating the actual computation; the zero label that previously flowed through `64` must be undone, and labels to other nodes recomputed, not just in explanation but in the computation itself. In principle we could return earlier once the explanation has settled (it does not depend on the other labels); in practice this is hard to see because the label outputs feed back into the explanation infrastructure.

## A second example: stable matching

For a more exotic and technically challenging example, we also have an implementation of [stable matching](https://en.wikipedia.org/wiki/Stable_marriage_problem). Stable matching takes a bipartite graph as input, where each node on one side of the graph has a rank-ordered preference for neighbors on the other side of the graph. The computation repeatedly has one side propose to the other, each node choosing its most appealing neighbor who has not yet rejected their proposal. The recipients collect their proposals and reject any that are not the best they've received.

This is an iterative computation, in the same spirit as prioritized label propagation for connected components. We can run this program in differential dataflow, and it will update as the preferences change.

We can run this example with explanations as well:

	Echidnatron% cargo run --example interactive-stable
	     Running `target/debug/examples/interactive-stable`

	initialization elapsed:	Duration { secs: 0, nanos: 1323 }
	> prefs + 0 0 2 1
	round 1 elapsed:	Duration { secs: 0, nanos: 18263025 }
	> prefs + 0 1 3 0
	round 2 elapsed:	Duration { secs: 0, nanos: 6258544 }
	> prefs + 0 2 4 0
	round 3 elapsed:	Duration { secs: 0, nanos: 6280442 }
	> prefs + 1 0 2 0
	round 4 elapsed:	Duration { secs: 0, nanos: 21216388 }

These preferences have the form `(id1, pref1, id2, pref2)` so we've described `0` and three nodes it might like to meet (`2`, then `3`, then `4`). Also, apparently nodes `1` and `2` have a thing for each other, and are probably going to hook up. Indeed, `0` and `3` get matched, and we might want to know why:

	> query + 0 1 3 0
	prefs_must:	((0, 0, 2, 1), 1)
	prefs_must:	((1, 0, 2, 0), 1)
	prefs_must:	((0, 1, 3, 0), 1)
	round 5 elapsed:	Duration { secs: 0, nanos: 120708979 }

These three preferences explain the output in that when the computation is run on them the corresponding output is produced. The tuple `(0, 2, 4, 0)` is not needed, because that tuple played no role in producing the queried output. However, the explanation above is non-minimal in a few ways, both good and bad:

1.	The example is non-minimal in that we could have just had `(0, 1, 3, 0)` as the only tuple. That would be pretty unilluminating, and we could "fix" this by insisting that `(0, 0, 2, 1)` be in the explanatory input. The explanation infrastructure permits the mandatory inclusion of any elements from the input, and it will sort out what further inputs you might need. 

	Note: in monotonic computations this ability is not very helpful, because adding more inputs cannot remove an output. If I insist on some additional input elements, the computation still produces at least the records it produced before, so an additional element cannot invalidate an explanation. Explaining non-monotonic computations is one of the neat new aspects of this framework.

2.	The example is also non-minimal in that we really don't need to know about `(0, 0, 2, 1)` at all. It is a possible pairing, but it is then removed. So, when looking back about what happened we might conclude that we don't need to know about it at all. That makes a lot of sense, and is something to aim at in the future. Right now the explanations are conservative, because it is easier for the implementors to understand, but they could likely be improved with better logic for identifying relevant differences. 

As with connected components, you may specify a graph filename. In the absence of better information, nodes prefer each other based on their node identifier, so small identifier nodes are most popular. This is not what we did in the paper where preferences were chosen randomly, but it makes it harder to visually understand what is happening.

Let's load up the graph and ask about the final matching for our trusty node `123456`, which happens to be to node `156689`:

	Echidnatron% cargo run --release --example interactive-stable -- ~/Projects/Datasets/livejournal
	     Running `target/release/examples/interactive-stable /Users/mcsherry/Projects/Datasets/livejournal`

	initialization elapsed:	Duration { secs: 0, nanos: 376 }
	query + 123456 156689 156689 123456
	prefs_must:	((18854, (26944, 26944, 18854)), 1)
	prefs_must:	((30824, (30851, 30851, 30824)), 1)
	prefs_must:	((83623, (133128, 133128, 83623)), 1)
	prefs_must:	((30824, (90896, 90896, 30824)), 1)
	prefs_must:	((21350, (30851, 30851, 21350)), 1)
	prefs_must:	((30820, (30870, 30870, 30820)), 1)
	prefs_must:	((123456, (156689, 156689, 123456)), 1)
	prefs_must:	((30693, (30870, 30870, 30693)), 1)
	prefs_must:	((83623, (90896, 90896, 83623)), 1)
	prefs_must:	((30824, (30870, 30870, 30824)), 1)
	prefs_must:	((24181, (91222, 91222, 24181)), 1)
	prefs_must:	((83623, (91222, 91222, 83623)), 1)
	prefs_must:	((30824, (44426, 44426, 30824)), 1)
	prefs_must:	((30820, (44426, 44426, 30820)), 1)
	prefs_must:	((123456, (133128, 133128, 123456)), 1)
	prefs_must:	((24181, (26944, 26944, 24181)), 1)
	round 1 elapsed:	Duration { secs: 0, nanos: 48247995 }

Ok, that is many graph edges. Not 68 million edges, which is the size of the graph, but certainly more than just one or two edges.

Here is a more appealing way to visualize the results in this setting. Since there is a total order on preferences, we can just have the most appealing people go in order, matching with their first remaning choice. Reformatting the output above, we can just read out:

	(18854, 26944) 		match!
	(21350, 30851) 		match!
	(24181, 26944) 		taken (by 18854)
	(24181, 91222) 		match!
	(30693, 30870) 		match!
	(30820, 30870) 		taken (by 30693)
	(30820, 44426)		match!
	(30824, 30870)		taken (by 30693)
	(30824, 30851)		taken (by 21350)
	(30824, 44426)		taken (by 30820)
	(30824, 90896)		match!
	(83623, 90896)		taken (by 30824)
	(83623, 91222)		taken (by 91222)
	(83623, 133128)		match!
	(123456, 133128)	taken (by 83623)
	(123456, 156689)	match!

So, that is an explanation for why `123456` and `156689` are now together, fit for daytime soaps. Imagine actually trying to keep all this in your head as you write query after query to sort out what caused this to happen. 

## Todo

The framework is not currently in a state that makes it a delight to try out new computations. The nested structure of the dataflow computation is exposed to the programmer, rather than concealed behind abstraction. In principle this could be fixed, though several of the type signatures in error messages no longer fit in one screen.

At present, the explanation infrastructure explains the full history of a record. It seems reasonable to look for other types of explanations, for example only explaning a record up to an indicated time, or exactly at a time, etc. This is not trivially done with the current framework, and it may require careful thought to sort out what seem like ambiguities in dependence tracking (e.g. when records may cancel, not requiring explanation, which do we cancel).

## Acknowledgements

This work was done with Zaheer Chothia, John Liagouris, and Mothy Roscoe while at [ETH Zürich](https://www.ethz.ch)'s [Systems Group](https://www.systems.ethz.ch). They should have a project page about this work relatively soon, and this text will link at it once it is ready.