object currying_excercise {
  def product(f: Int => Int)(a: Int, b: Int): Int =
    if (a > b) 1
    else f(a) * product(f)(a+1, b)

  product(x => x*x)(2,4)

}