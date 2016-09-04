#[macro_use]
extern crate explanation;

#[allow(unused_variables)]
extern crate rand;
extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::cell::RefCell;
use std::io::BufRead;

use graph_map::GraphMMap;
use timely::dataflow::*;
use timely::dataflow::scopes::Child;
use timely::dataflow::operators::*;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;

use explanation::{Variable, MonotonicVariable};

fn main() {

    timely::execute_from_args(std::env::args(), move |root| {

        // BEGIN DATAFLOW CONSTRUCTION
        // Outer-most streaming scope; here inputs to the graph, labels, queries, etc may change.
        let (mut prefs, mut query, probe) = root.scoped::<u32, _, _>(move |streaming| {

            // Construct inputs for prefence data and queries made against the results.
            let (prefs_handle, prefs) = streaming.new_input(); let prefs = Collection::new(prefs);
            let (query_handle, query) = streaming.new_input(); let query = Collection::new(query);

            // Iterative scope for rounds of input correction
            let mut prefs_must = streaming.scoped::<u32,_,_>(move |correction| {

                // Bring each input into the scope.
                let prefs = prefs.enter(correction);
                let query = query.enter(correction);

                // Each data input uses a MonotonicVariable to track its elements required to explain outputs.
                // These collections grow monotonically in each round of correction, limited by the full set.
                let mut prefs_must = MonotonicVariable::new(correction);

                // Scope for explanation derivation.
                let child_scope = RefCell::new(correction.new_subscope());
                let child_index = child_scope.borrow().index;

                // determine and return necessary members of `prefs`.
                let prefs_need = {

                    // wrap an explanation scope builder.
                    let mut explanation_scope = Child {
                        subgraph: &child_scope,
                        parent: correction.clone(),
                    };
            
                    // define variables for each input to the computation.
                    // the data source is from outside the correction loop,
                    // and the working source is the *_need stream.
                    let mut var_prefs = Variable::new(prefs.clone(), prefs_must.stream.clone(), &mut explanation_scope);

                    // computation loop;
                    let mut final_prefs = correction.scoped::<u32,_,_>(|inner| {

                        // BEGIN FEEDBACK SETUP
                        let (handle1, cycle1) = inner.loop_variable(u32::max_value(), 1); let cycle1 = Collection::new(cycle1);
                        let (handle2, cycle2) = inner.loop_variable(u32::max_value(), 1); let cycle2 = Collection::new(cycle2);
                        let mut var_rejections = Variable::new(cycle1, cycle2, &mut explanation_scope);
                        // END FEEDBACK SETUP

                        // proposals are `var_prefs` excluding any rejections.
                        let mut var_entered = var_prefs.enter(inner);
                        let mut var_options = except!(var_entered, var_rejections, explanation_scope);

                        // have each individual propose to its most appealing option.
                        let mut var_proposals = min!(var_options, |x| x, explanation_scope);

                        // rotate preferences to be keyed by recipient, ordered by their preference; take min; rotate back.
                        let mut var_accepts1 = var_proposals.map_inverse(|(a,(c,b,d))| (b,(d,a,c)), |(b,(d,a,c))| (a,(c,b,d)));
                        let mut var_accepts2 = min!(var_accepts1, |x| x, explanation_scope);
                        let mut var_accepts = var_accepts2.map_inverse(|(b,(d,a,c))| (a,(c,b,d)), |(a,(c,b,d))| (b,(d,a,c)));

                        // var_accepts.stream.inspect_batch(|t,xs| println!("accepts: {:?}\t{:?}", t, xs));

                        // rejected proposals should be fed back around
                        let mut var_rejected = except!(var_proposals, var_accepts, explanation_scope)
                                                .concat(&mut var_rejections)
                                                .consolidate();

                        // var_rejected.stream.inspect_batch(|t,xs| println!("rejects: {:?}\t{:?}", t, xs));

                        // BEGIN FEEDBACK CONNECT
                        var_rejected.stream.inner.connect_loop(handle1);
                        var_rejected.working.inner.connect_loop(handle2);
                        var_rejected.depends.add(
                            &var_rejections.depends.stream
                            .filter(|&(_,_,t,_)| t.inner > 0)
                            .map(|(a,cbd,t,q)| (a,cbd,Product::new(t.outer, t.inner - 1),q))
                        );
                        // END FEEDBACK CONNECT

                        // accepted proposals are what we want to keep.
                        leave!(var_accepts, explanation_scope)
                    });

                    // final_prefs.stream.filter(|x| x.0 == 123456).inspect(|x| println!("{:?}", x));

                    // introduce any query elements as initial dependences.
                    final_prefs.depends.add(&query.enter(&explanation_scope));

                    // pop input requirements out of the explanation scope and return them.
                    var_prefs.depends.stream.leave()
                };

                // all explanation infrastructure in place; add to correct scope.
                correction.add_operator_with_index(child_scope.into_inner(), child_index);

                // intersect required edges and labels with existing edges and labels.
                prefs_must.add(&prefs_need.map(|(k,v,_t,_q)| ((k,v),())).semijoin(&prefs).map(|((k,v),_)| (k,v)));

                // merge the things we need, pop them out of the loop, and probe
                prefs_must.stream.leave()
            });

            // print out what we require from each input.
            prefs_must = prefs_must.inspect(|x| println!("prefs_must:\t{:?}", x));

            // attach a probe, so that we can await completeness.
            let query_probe = prefs_must.probe().0;

            (prefs_handle, query_handle, query_probe)
        });
        // END DATAFLOW CONSTRUCTION

        // BEGIN DATA LOADING
        // NOTE: This could be replaced with your favorite data format.
        if let Some(filename) = std::env::args().nth(1) {
            let edges = GraphMMap::new(&filename);
            for node in 0..edges.nodes() {
                if node % root.peers() == root.index() {
                    for &edge in edges.edges(node) {
                        prefs.send(((node as u32, (edge as u32, edge as u32, node as u32)), 1));
                    }
                }
            }
        }
        // END DATA LOADING

        // close labels, advance graph and query inputs to the next epoch.

        prefs.advance_to(1);
        query.advance_to(1);
        root.step_while(|| probe.lt(&query.time()));
        println!("");

        let timer = ::std::time::Instant::now();
        root.step_while(|| probe.lt(&query.time()));
        if root.index() == 0 { println!("initialization elapsed:\t{:?}", timer.elapsed()); }
        
        let mut round = 1;
        let input = std::io::stdin();
        for line in input.lock().lines().map(|x| x.unwrap()) {
        
            let mut elts = line[..].split_whitespace();
        
            if let Some(command) = elts.next() {
                if command == "query" {
                    if let Some(sign) = elts.next() {
                        let sign = if sign == "-" { -1i32 } else { 1 };
                        if let Some(source) = elts.next() {
                            if let Some(node) = source.parse::<u32>().ok() {
                                if let Some(pref1) = elts.next() {
                                    if let Some(pref1) = pref1.parse::<u32>().ok() {
                                        if let Some(target) = elts.next() {
                                            if let Some(target) = target.parse::<u32>().ok() {
                                                if let Some(pref2) = elts.next() {
                                                    if let Some(pref2) = pref2.parse::<u32>().ok() {
                                                        query.send(((
                                                            node, 
                                                            (pref1, target, pref2), 
                                                            Product::new(RootTimestamp::new(0), u32::max_value()),
                                                            0 as u32
                                                        ),sign));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                            }
                        }
                    }
                }
                // format: "prefs {+,-} a_id a_pref b_id b_pref"
                if command == "prefs" {
                    if let Some(sign) = elts.next() {
                        let sign = if sign == "-" { -1i32 } else { 1 };
                        if let Some(source) = elts.next() {
                            if let Some(source) = source.parse::<u32>().ok() {
                                if let Some(pref1) = elts.next() {
                                    if let Some(pref1) = pref1.parse::<u32>().ok() {
                                        if let Some(target) = elts.next() {
                                            if let Some(target) = target.parse::<u32>().ok() {
                                                if let Some(pref2) = elts.next() {
                                                    if let Some(pref2) = pref2.parse::<u32>().ok() {
                                                        prefs.send(((source, (pref1,target,pref2)),sign));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
        
                prefs.advance_to(round + 1);
                query.advance_to(round + 1);
                let timer = ::std::time::Instant::now();
                root.step_while(|| probe.lt(&query.time()));
                if root.index() == 0 {
                    println!("round {:?} elapsed:\t{:?}", round, timer.elapsed());
                }
        
                round += 1;
            }
        }
    }).unwrap();
}
