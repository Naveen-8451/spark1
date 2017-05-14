package org.test.spark1

object ex extends App{
  for(i<- 1 to 3; j<-1 to 3; k <-1 to 5) 
    print((100*k + 10*i + j))
}