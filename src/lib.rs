//! Infrastructure for tracking explanations of differential dataflow computations.

#[allow(unused_variables)]
extern crate fnv;
extern crate rand;
extern crate time;
extern crate timely;
extern crate timely_sort;
extern crate graph_map;
extern crate differential_dataflow;

use std::rc::Rc;
use std::hash::Hash;

use timely::progress::Timestamp;

use timely::dataflow::*;
use timely::dataflow::scopes::Child;
use timely::dataflow::operators::*;
use timely::dataflow::operators::feedback::Handle;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;

use timely_sort::Unsigned;

use differential_dataflow::{Data, Collection, Delta};
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

/// A explanation-tracking collection.
///
/// A `Variable` represents a differential dataflow collection, but also two additional collections corresponding to 
/// 
/// * Those elements required as part of explaining some outputs, and 
/// * Those elements currently reproduced using explanatory inputs.
///
/// A `Variable` supports many of the same operations that a `Collection` supports, which perform additional work to
/// maintain the explanation dataflow infrastructure.
pub struct Variable<
    'a,
    G: Scope, 
    K: Data+Default, 
    V: Data+Default, 
    Gp: Scope<Timestamp=Product<Product<RootTimestamp, u32>, u32>>> 
where G::Timestamp: Ord+Hash {
    /// The collection itself.
    pub stream: Collection<G, (K, V)>,
    /// A collection of elements produced by explanatory inputs.
    pub working: Collection<G, (K, V)>,
    /// A collection of elements required for explanation.
    pub depends: MonotonicVariable<'a, Gp, (K, V, G::Timestamp, u32)>,
}

impl<'a,
     G: Scope, 
     K: Data+Default, 
     V: Data+Default, 
     Gp: Scope<Timestamp=Product<Product<RootTimestamp, u32>, u32>>> 
Variable<'a, G, K, V, Gp> where G::Timestamp: Ord+Hash {
    /// Constructs a new `Variable` from collections and the explanation-tracking scope.
    pub fn new(
        source: Collection<G, (K, V)>, 
        working: Collection<G, (K, V)>, 
        prov: &mut Child<'a, Gp, u32>) -> Variable<'a, G, K, V, Gp> {

        Variable {
            stream: source,
            working: working,
            depends: MonotonicVariable::new(prov),
        }
    }
}

#[macro_export]
macro_rules! lift {
    ($stream:expr) => {{
        Collection::new(
            $stream.consolidate()
                   .inner
                   .unary_stream(timely::dataflow::channels::pact::Pipeline, "lifting", |input, output| {

                while let Some((time, data)) = input.next() {
                    let mut session = output.session(&time);
                    for &(ref datum, _weight) in data.iter() {
                        session.give(((datum.clone(), time.time()), 1));
                    }

                    // for &(ref datum, weight) in data.iter() {
                    //     session.give(((datum.clone(), time.time()), weight));
                    // }
                }
            })
        )
    }}
}


impl<'a,
     G: Scope, 
     K: Data+Default, 
     V: Data+Default, 
     Gp: Scope<Timestamp=Product<Product<RootTimestamp, u32>, u32>>> 
Variable<'a, G, K, V, Gp> where G::Timestamp: Ord+Hash+Lattice {
    /// Joins two collections using an unsigned key.
    pub fn join_u<V2: Unsigned+Default+Data>(&mut self, other: &mut Variable<'a, G, K, V2, Gp>) -> Variable<'a, G, K, (V, V2), Gp> 
        where K : Unsigned {

        let result = Variable::new(
            self.stream.join_u(&other.stream).map(|(x,y,z)| (x,(y,z))),
            self.working.join_u(&other.working).map(|(x,y,z)| (x,(y,z))),
            &mut self.depends.scope()
        );

        // add each component of joined results to the requirements of each input
        self.depends.add(&result.depends.stream.map(|(x,(y,_),t,q)| (x,y,t,q)));
        other.depends.add(&result.depends.stream.map(|(x,(_,z),t,q)| (x,z,t,q)));
        result

    }

    /// Maps elements of one collection to another using an invertible function (and its inverse).
    pub fn map<K2: Data+Default, 
               V2: Data+Default, 
               F1: Fn((K,V))->(K2,V2)+'static, 
               F2: Fn((K2,V2,G::Timestamp,u32))->(K,V,G::Timestamp,u32)+'static>(&mut self, logic: F1, inverse: F2) -> 
               Variable<'a, G, K2, V2, Gp>
           {

        let forward = Rc::new(logic);
        let clone1 = forward.clone();
        let clone2 = forward.clone();
        let result = Variable::new(
            self.stream.map(move |x| clone1(x)), 
            self.working.map(move |x| clone2(x)), 
            &mut self.depends.scope()
        );

        self.depends.add(&result.depends.stream.map(inverse));
        result

    }

    /// Concatenates two collections.
    pub fn concat(&mut self, other: &mut Variable<'a, G, K, V, Gp>) -> Variable<'a, G, K, V, Gp> {

        let result = Variable::new(
            self.stream.concat(&other.stream), 
            self.working.concat(&other.working), 
            &mut self.depends.scope()
        );

        self.depends.add(&result.depends.stream);
        other.depends.add(&result.depends.stream);
        result

    }

    /// Brings a collection from an outer scope into a child scope.
    pub fn enter<'b, T: Timestamp+Data>(&mut self, child: &Child<'b, G, T>) -> Variable<'a, Child<'b,G,T>, K, V, Gp> {

        let result = Variable::new( self.stream.enter(child), self.working.enter(child), &mut self.depends.scope() );
        self.depends.add(&result.depends.stream.map(|(x,y,t,q)| (x,y,t.outer,q)));
        result

    }

    /// Brings a collection from an outer scope into a child scope, each element at its own timestamp.
    pub fn enter_at<'b, T: Timestamp+Data, F: Fn(&((K,V), Delta))->T+'static>(&mut self, child: &Child<'b,G, T>, at: F) -> Variable<'a, Child<'b,G,T>, K, V, Gp> {

        let at = Rc::new(at);
        let clone1 = at.clone();
        let clone2 = at.clone();

        let result = Variable::new( 
            self.stream.enter_at(child, move |x| clone1(x)), 
            self.working.enter_at(child, move |x| clone2(x)), 
            &mut self.depends.scope() 
        );

        self.depends.add(&result.depends.stream.map(|(x,y,t,q)| (x,y,t.outer,q)));
        result

    }
}

#[macro_export]
macro_rules! min {
    ($var:expr, $logic:expr, $scope:expr) => {{

        // compute the minimums for both the actual and working data collections.
        let min1 = $var.stream.group_u(|_k, s, t| t.push(((*s.next().unwrap().0), 1)));
        let min2 = $var.working.group_u(|_k, s, t| t.push(((*s.next().unwrap().0), 1)));

        // let min1 = min1.inspect(|x| println!("min1: {:?}", x));
        // let min2 = min2.inspect(|x| println!("min2: {:?}", x));

        // construct a new variable from these minimums.
        let var_min = Variable::new(
            min1.map(|(k,v)| (k,$logic(v))),
            min2.map(|(k,v)| (k,$logic(v))),
            &mut $scope
        );

        // extract minimums and presents them as explainable data, in the explanation scope.
        let temp = lift!(min1.concat(&min2)).leave().enter(&$scope).map(|((x,(l,y)),t)| (x,((l,y),t)));
        // let temp = lift!($var.stream.concat(&$var.working)).leave().enter(&$scope).map(|((x,(l,y)),t)| (x,((l,y),t)));

        // set explanation requirements from requests by
        //  (i)     joining requests against actual minimums, 
        //  (ii)    filtering records to only those with less or equal time,
        //  (iii)   filtering records to only those with less or equal value,
        $var.depends.add(
            &temp.join_u(&var_min.depends.stream.map(|(x,l,t,q)| (x,(l,t,q)))
                                                // .inspect_batch(|t,xs| println!("min_req: {:?} @ {:?}", xs, t))
                                                )  // (i)
                 .filter(|&(_,(_,t1),(_,t2,_))| t1 <= t2)                       // (ii)
                 .filter(|&(_,((l1,_),_),(l2,_,_))| l1 <= l2)                   // (iii)
                 .map(|(x,((l,y),t),(_,_,q))| (x,(l,y),t,q))                    // reformatting
        );

        var_min
    }}
}

#[macro_export]
macro_rules! leave {
    ($var:expr, $scope:expr) => {{
        let result = Variable::new( $var.stream.leave(), $var.working.leave(), &mut $scope );
        $var.depends.add(
            &result.depends.stream
                .map(|(x,y,t,q)| ((x,y),(t,q)))
                // .inspect_batch(|t,xs| println!("leave1: {:?} @ {:?}", xs, t))
                .join(&lift!($var.stream.concat(&$var.working)).leave().enter(&$scope))
                // .inspect_batch(|t,xs| println!("leave2: {:?} @ {:?}", xs, t))
                .map(|((x,y),(_,q),t)| (x,y,t,q))
        );
        result
    }}
}

/// A collection defined by multiple mutually recursive rules.
pub struct MonotonicVariable<'a, G: Scope, D: Data+Default>
where G::Timestamp: Lattice {
    pub feedback: Option<Handle<G::Timestamp, u32,(D, i32)>>,
    pub stream:  Collection<Child<'a, G, u32>, D>,
    pub current:  Collection<Child<'a, G, u32>, D>,
}

impl<'a, G: Scope, D: Data+Default> MonotonicVariable<'a, G, D> where G::Timestamp: Lattice {
    /// Creates a new `Variable` and a `Stream` representing its output, from a supplied `source` stream.
    pub fn new(scope: &mut Child<'a, G, u32>) -> MonotonicVariable<'a, G, D> {
        let (feedback, cycle) = scope.loop_variable(u32::max_value(), 1);
        let cycle = Collection::new(cycle);
        MonotonicVariable { feedback: Some(feedback), stream: cycle.clone(), current: cycle.clone() }
    }
    /// Adds a new source of data to the `Variable`.
    pub fn add(&mut self, source: &Collection<Child<'a, G, u32>, D>) {
        self.current = self.current.concat(source);
    }
    pub fn scope(&self) -> Child<'a, G, u32> {
        self.current.scope()
    }
}

impl<'a, G: Scope, D: Data+Default> Drop for MonotonicVariable<'a, G, D> where G::Timestamp: Lattice {
    fn drop(&mut self) {
        if let Some(feedback) = self.feedback.take() {
            self.current.threshold(|_, w| if w > 0 { 1 } else { 0 })
                        .inner
                        .connect_loop(feedback);
        }
    }
}

/// Container for feedback edges for a explanation-traced variable.
pub struct VariableFeedback<'a, G, K, V, Gp> 
where G: Scope, 
      K: Data+Default, 
      V: Data+Default, 
      Gp: Scope<Timestamp=Product<Product<RootTimestamp, u32>, u32>>,
      G::Timestamp: Ord+Hash {
    handles: Option<(Handle<G::Timestamp, u32, ((K,V), i32)>,
                     Handle<G::Timestamp, u32, ((K,V), i32)>)>,
    variable: Variable<'a, Child<'a, G, u32>, K, V, Gp>,
}

impl<'a, G, K, V, Gp> VariableFeedback<'a, G, K, V, Gp>
where G: Scope, 
      K: Data+Default, 
      V: Data+Default, 
      Gp: Scope<Timestamp=Product<Product<RootTimestamp, u32>, u32>>,
      G::Timestamp: Ord+Hash {
    pub fn new(scope: &mut Child<'a, G, u32>, explanation_scope: &mut Child<'a, Gp, u32>) -> Self {
        let (handle1, cycle1) = scope.loop_variable(u32::max_value(), 1); let cycle1 = Collection::new(cycle1);
        let (handle2, cycle2) = scope.loop_variable(u32::max_value(), 1); let cycle2 = Collection::new(cycle2);
        VariableFeedback { 
            handles: Some((handle1, handle2)),
            variable: Variable::new(cycle1, cycle2, explanation_scope), 
        }
    }
    pub fn set(&mut self, source: &mut Variable<'a, Child<'a, G, u32>, K, V, Gp>) {  
        if let Some((handle1, handle2)) =  self.handles.take() {
            source.stream.inner.connect_loop(handle1);
            source.working.inner.connect_loop(handle2);
            source.depends.add(
                &self.variable.depends.stream
                .filter(|&(_,_,t,_)| t.inner > 0)
                .map(|(x,l,t,q)| (x,l,Product::new(t.outer, t.inner - 1),q))
            );
        }
    }
}

impl<'a, G, K, V, Gp> ::std::ops::Deref for VariableFeedback<'a, G, K, V, Gp>
where G: Scope, 
      K: Data+Default, 
      V: Data+Default, 
      Gp: Scope<Timestamp=Product<Product<RootTimestamp, u32>, u32>>,
      G::Timestamp: Ord+Hash {
        type Target = Variable<'a, Child<'a, G, u32>, K, V, Gp>;
        fn deref(&self) -> &Self::Target {
            &self.variable
        }
}


impl<'a, G, K, V, Gp> ::std::ops::DerefMut for VariableFeedback<'a, G, K, V, Gp>
where G: Scope, 
      K: Data+Default, 
      V: Data+Default, 
      Gp: Scope<Timestamp=Product<Product<RootTimestamp, u32>, u32>>,
      G::Timestamp: Ord+Hash {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.variable
        }
}
