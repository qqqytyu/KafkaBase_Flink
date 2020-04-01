class StaticTest {

    def test = {
        val static1: StaticTest.myTestClass = new StaticTest.myTestClass("hello")
        val static2: StaticTest.myTestClass = new StaticTest.myTestClass("word")
        println(s"s1: ${static1.getStr}")
        println(s"s2: ${static2.getStr}")
        println(s"s1 hash: ${static1.hash}")
        println(s"s2 hash: ${static2.hash}")
        println(s"hash1: ${System.identityHashCode(static1)}")
        println(s"hash2: ${System.identityHashCode(static2)}")
    }

}

object StaticTest{

    def main(args: Array[String]): Unit = {
        val test: StaticTest = new StaticTest()
        test.test
    }

    class myTestClass(str: String){

        def getStr: String = this.str

        def hash: Int = System.identityHashCode(this.str)

    }

}
