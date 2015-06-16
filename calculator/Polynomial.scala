package calculator

object Polynomial {
  
  //returns the delta of a quadratic root
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = Signal {
    b()*b() - 4*a()*c()
  }

  //returns the set of solutions in a quadratic equation
  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = Signal {
    if (delta() < 0) Set()
    else if (delta() == 0) Set((-b() - math.sqrt(delta())) / (2*a())) 
    else Set((-b() - math.sqrt(delta())) / (2*a()), (-b() + math.sqrt(delta())) / (2*a()))
  }
}
