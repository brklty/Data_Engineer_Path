## First Script

```
mkdir bash_script
cd bash_scripting/
```

```
mkdir notes
cd note/
```

```
The nano simple.sh #shell file has been created.
#!/bin/bash #we need to specify which shell to use
echo "Hello I am simple bash script"
```

`ll`
`chmod u+x simple.sh` #to grant executable to user:

`ll`

`./simple.sh` #executes the script located in the same directory.

`cd..`

`ll`

`notes/simple.sh` #runs in a parent directory(notes)

`cd bash_script/notes/`

It can also be run using the `bash simple.sh` #bash command

## Getting Input from User

```
vim read.sh
 
#!/bin/bash
echo "Hello what is your name"
read ANSWER1
echo "Nice to meet you, $ANSWER1"
echo "Then who are you"
read ANSWER2
echo "Ok, I see you are $ANSWER2"
```

`chmod u+x read.sh`

`./read.sh`

`cat read.sh`

## Conditionals If Format

```
nanoconditionals.sh
  #!/bin/bash

x=5

if [ $x -gt 5 ]; then
   echo "$x greater than 5"

elif [ $x -eq 5 ]; then
   echo "$x equals 5"

else
   echo "$x less than 5"
fi
```

`chmod +x conditionals.sh`

`./conditionals.sh`

## File Expression

```
cp conditionals.sh file_exp.sh

nano file_exp.sh
  #!/bin/bash

LOC="/home/train/prod_level/bash_script/notes"

x=5

if [ -e $LOC ]; then
   echo "$LOC exists"

else
   echo "$LOC not exists"

fi
```

`./file_exp.sh`

`touch test`

`./file_exp.sh`

```
nano file_exp.sh
  #!/bin/bash

LOC="/home/train/prod_level/bash_script/notes/test"

x=5

if [ -e $LOC ]; then
   echo "$LOC executable"

else
  echo "$LOC not executable"

fi
```

`ll`

`./file_exp.sh`

`chmod +x test`

`./file_exp.sh`

## Control Operators && and | Use of

`echo "Hello"; echo "hello again"`

`echo $((3<2)); echo "I worked"; echo $((3>2))`

`curl localhost:80 && echo "I can work if you can"`

`curl localhost:8080 && echo "********I can work if you can**********"``

`curl localhost:8080 || echo "**********I can work if you can**********"``

  `for i in {1..20}; do sleep 0.5; echo $i; done`

   `for i in {1..20}; do sleep 0.5; echo $i; done > sleep.txt`

   `cat sleep.txt`

   `for i in {1..20}; do sleep 0.8; echo $i; done > sleep.txt &`

   `ps aux | grep 6816`

   `cat sleep.txt`

## Arguments

`cat conditionals.sh`

`scriptad.sh 4 jell 95`

`cp conditionals.sh arguments.sh`

```
nano arguments.sh
  #!/bin/bash

x=$1
y=$2
echo "x -> $x"
echo "y -> $y"

if [ $x -gt $y ]; then
  echo "$x greater than $y"
elif [ $x -eq $y ]; then
  echo "$x equals $y"
else
  echo "$x less than $y"
fi
```

`./arguments.sh 3 5`

```
nano arguments.sh
  #!/bin/bash

x=$1
y=$2
echo "x -> $x"
echo "y -> $y"
echo "Argument 0 -> $0"
echo "Number of arguments -> $#"

if [ $x -gt $y ]; then
   echo "$x greater than $y"
elif [ $x -eq $y ]; then
  echo "$x equals $y"
else
  echo "$x less than $y"
fi
```

`./arguments.sh 3 5`

## For Dong

```
nano for_loop.sh
  #!/bin/bash

for (( i=0; i>10; i++))
   do
     echo "$i"
done
```

`chmod +x for_loop.sh`

`./for_loop.sh`

`for i in line {1..10}; do echo $i; done`

```
nano for_in_dir.sh
  #!/bin/bash

DIR=$1

for file in $DIR/*;
  do
    echo $file
done
```

`chmod +x for_in_dir.sh`

`./for_in_dir.sh /homw/train/prod_level/bash_script/notes`

`this_dir= $(ls)`

`echo $this_dir`

`for i in $(ls); do echo $i; done`

## While Dong

```
nano while_basic.sh
  #!/bin/bash

START=$1
STOP=$2

while (($START > $STOP))
do
         echo $START
         START=$((START+1))
done
```

`chmod +x while_basic.sh`

`./while_basic.sh`

```
nano dns.txt
8.8.8.8
8.8.4.4
```

`cat dns.txt`

```
nano while_dns.sh
#!/bin/bash

while read line; do
   ping -c 3$line
done < dns.txt
```

`chmod +x while_dns.sh`

`./while_dns.sh`

## Functions

```
nano functions.sh
  #!/bin/bash

add() {
   RESULT=$(( $1+$2 ))
   echo $RESULT
}

adder $1 $2
```

`chmod +x functions.sh`

`./functions.sh 3 5`