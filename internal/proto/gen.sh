#bin/bash

if  [ -d  "akka"  ]; then
  rm -r akka
fi

if  [ -d  "schedulerx"  ]; then
  rm -r schedulerx
fi

protoc --go_out=. *.proto