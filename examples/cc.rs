#[macro_use]
extern crate explanation;

#[allow(unused_variables)]
extern crate rand;
extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::rc::Rc;                        // used to capture output so that we can query it; 
use std::cell::RefCell;                 // perhaps use `capture` instead?
use rand::{StdRng, Rng, SeedableRng};   // used to drive random updates to the graph.
use graph_map::GraphMMap;               // for reading graph input (binary format).

use timely::dataflow::*;
use timely::dataflow::scopes::Child;
use timely::dataflow::operators::*;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;

use differential_dataflow::Collection;
use differential_dataflow::operators::*;

use explanation::{Variable, MonotonicVariable, VariableFeedback};

fn main() {

    // Read parameters: graph filename, whether to issue queries, and rate of updates.
    let filename = std::env::args().nth(1).unwrap();
    let queries = std::env::args().nth(2).unwrap().parse::<bool>().unwrap();
    let updates = std::env::args().nth(3).unwrap().parse::<usize>().unwrap();

    timely::execute_from_args(std::env::args(), move |root| {

        // Shared space to record and read output records.
        let derived1 = Rc::new(RefCell::new(Vec::new()));
        let derived2 = derived1.clone();

        // BEGIN DATAFLOW CONSTRUCTION
        // Outer-most streaming scope; here inputs to the graph, labels, queries, etc may change.
        let (mut graph, mut label, mut query, probe) = root.scoped::<u32, _, _>(move |streaming| {

            // Construct inputs for graph data, label data, and queries made against the results.
            // NOTE: label data supplied separately as per other systems, which provide graph node
            // NOTE: data independently from the graph; otherwise we would compute and maintain it.
            let (graph_handle, graph) = streaming.new_input(); let graph = Collection::new(graph);
            let (label_handle, label) = streaming.new_input(); let label = Collection::new(label);
            let (query_handle, query) = streaming.new_input(); let query = Collection::new(query);

            // Iterative scope for rounds of input correction
            let (mut graph_must, mut label_must) = streaming.scoped::<u32,_,_>(move |correction| {

                // Bring each input into the scope.
                let graph = graph.enter(correction);
                let label = label.enter(correction);
                let query = query.enter(correction);

                // Each data input uses a MonotonicVariable to track its elements required to explain outputs.
                // These collections grow monotonically in each round of correction, limited by the full set.
                let mut graph_must = MonotonicVariable::new(correction);
                let mut label_must = MonotonicVariable::new(correction);

                // Scope for explanation derivation.
                let child_scope = RefCell::new(correction.new_subscope());
                let child_index = child_scope.borrow().index;

                // determine and return necessary members of `graph` and `label`.
                let (graph_need, label_need) = {

                    // wrap an explanation scope builder.
                    let mut explanation_scope = Child {
                        subgraph: &child_scope,
                        parent: correction.clone(),
                    };
            
                    // define variables for each input to the computation.
                    // the data source is from outside the correction loop,
                    // and the working source are the *_must collections.
                    let mut var_graph = Variable::new(graph.clone(), graph_must.stream.clone(), &mut explanation_scope);
                    let mut var_label = Variable::new(label.clone(), label_must.stream.clone(), &mut explanation_scope);

                    // transpose edges and concatenate, symmetrizing the graph.
                    let mut var_edges = var_graph.map(|(x,y)| (y,x), |(y,x,t,q)| (x,y,t,q))
                                                 .concat(&mut var_graph);

                    // actual computation loop; can you believe we do computation, too?
                    let mut final_labels = correction.scoped::<u32,_,_>(|inner| {

                        // BEGIN FEEDBACK SETUP
                        let (handle1, cycle1) = inner.loop_variable(u32::max_value(), 1); let cycle1 = Collection::new(cycle1);
                        let (handle2, cycle2) = inner.loop_variable(u32::max_value(), 1); let cycle2 = Collection::new(cycle2);
                        let mut var_inner = Variable::new(cycle1, cycle2, &mut explanation_scope);
                        // END FEEDBACK SETUP

                        // join edges with looped labels, then re-order to have dst as key
                        let mut var_transmit = 
                            var_edges.enter(inner)
                                     .join_u(&mut var_inner)
                                     .map(|(x,(y,l))| (y,(l,x)), |(y,(l,x),t,q)| (x,(y,l),t,q));

                        // bring in initial labels from outside, concat with proposals
                        let mut var_options = 
                            var_label.enter_at(inner, |r| 256 * (((((r.0).0) as f64).ln() * 10.0) as u32))
                                     .map(|(x,l)| (x,(l,x)), |(x,(l,_),t,q)| (x,l,t,q))
                                     .concat(&mut var_transmit);

                        // group the labels by key, using min! macro
                        let mut var_min = min!(var_options, |(l,_d)| l, explanation_scope);

                        // BEGIN FEEDBACK LOGIC
                        var_min.stream.inner.connect_loop(handle1);
                        var_min.working.inner.connect_loop(handle2);
                        var_min.depends.add(
                            &var_inner.depends.stream
                            .filter(|&(_,_,t,_)| t.inner > 0)
                            .map(|(x,l,t,q)| (x,l,Product::new(t.outer, t.inner - 1),q))
                        );
                        // END FEEDBACK LOGIC

                        leave!(var_min, explanation_scope)
                    });

                    // introduce any query elements as initial dependences.
                    final_labels.depends.add(&query.enter(&explanation_scope));

                    // record the outputs, so that we may request them as part of our experiment.
                    if queries {
                        final_labels.stream.consolidate_by(|x| x.0)
                                           .consolidate_by(|_| 0u32)
                                           .inspect_batch(move |_,x| {
                                                let mut derived = derived1.borrow_mut();
                                                for &y in x.iter() {
                                                    derived.push(y.0);
                                                }
                                            });
                    }

                    // pop input requirements out of the explanation scope and return them.
                    (var_graph.depends.stream.leave(), var_label.depends.stream.leave())
                };

                // all explanation infrastructure in place; add to correct scope.
                correction.add_operator_with_index(child_scope.into_inner(), child_index);

                // intersect required edges and labels with existing edges and labels.
                graph_must.add(&graph_need.map(|(k,v,_t,_q)| ((k,v),())).semijoin(&graph).map(|((k,v),_)| (k,v)));
                label_must.add(&label_need.map(|(k,v,_t,_q)| ((k,v),())).semijoin(&label).map(|((k,v),_)| (k,v)));

                // merge the things we need, pop them out of the loop, and probe
                (graph_must.stream.leave(), label_must.stream.leave())
            });

            // // optionally, print out what we require from each input.
            if std::env::args().find(|x| x == "inspect").is_some() {
                graph_must = graph_must.inspect(|x| println!("graph_must:\t{:?}", x));
                label_must = label_must.inspect(|x| println!("label_must:\t{:?}", x));
            }

            // attach a probe, so that we can await completeness.
            let probe = graph_must.concat(&label_must).probe().0;

            (graph_handle, label_handle, query_handle, probe)
        });
        // END DATAFLOW CONSTRUCTION

        // BEGIN DATA LOADING
        // NOTE: This could be replaced with your favorite data format.
        let edges = GraphMMap::new(&filename);
        for node in 0..edges.nodes() {
            if node % root.peers() == root.index() {
                if edges.edges(node).len() > 0 {
                    label.send(((node as u32, node as u32), 1));                        
                }
                for &edge in edges.edges(node) {
                    graph.send(((node as u32, edge as u32), 1));
                }
            }
        }
        drop(edges);
        // END DATA LOADING

        // close labels, advance graph and query inputs to the next epoch.
        label.close();
        graph.advance_to(1);
        query.advance_to(1);
        let timer = ::std::time::Instant::now();
        root.step_while(|| probe.lt(&query.time()));
        if root.index() == 0 { println!("initialization elapsed:\t{:?}", timer.elapsed()); }

        // random number generators for graph updates.
        let seed: &[_] = &[1, 2, 3, root.index()];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);
        let mut rng2: StdRng = SeedableRng::from_seed(seed);

        // repeatedly do updates and/or queries.
        for round in 1u32..1000 {
            if root.index() == 0 {

                // request explanation of a random output.
                if queries {
                    let index = rng1.gen_range(0, derived2.borrow().len());
                    let target = derived2.borrow()[index];
                    if std::env::args().find(|x| x == "inspect").is_some() {
                        println!("seeking explanation for {:?}", target);
                    }
                    query.send(((target.0, target.1, Product::new(RootTimestamp::new(0), u32::max_value()), round as u32),1));
                }

                // introduce new edges, chosen randomly.
                for _ in 0..updates {
                    graph.send(((rng2.gen_range(0, derived2.borrow().len() as u32), 
                                 rng2.gen_range(0, derived2.borrow().len() as u32)),1));
                }
            }

            let timer = ::std::time::Instant::now();
            graph.advance_to(round + 1);
            query.advance_to(round + 1);
            root.step_while(|| probe.lt(&query.time()));
            if root.index() == 0 {
                println!("round {:?} elapsed:\t{:?}\n", round, timer.elapsed());
            }
        }
    }).unwrap();
}