## Basic Commands

`mkdir prod_level` #mkdir dirname:creates folder

`cd prod_level` #used to change folder

`mkdir linux_basic`

```
mkdir notes
cd note/
```

`pwd` #prints the path to the working folder

`ls` #lists the files in the folder

`cd~` returns to #home directory

`pwd`

`ls -l` #detailed lists (alphabetical)

`ls -lh` #lists in a detailed but more readable way

`ls -lr` #more detailed lists (in reverse order)

`ls -ltr` #detailed lists (sorted by date)

`ls -lt` #detailed lists (sorted by date)

`ls -la` #hidden file is also shown. Hidden files. starts with

`clear` # Clears the page

`history` #lists commands used
`history 10` #Lists the last 10 commands used
`history -c` #Delete used command history

`cd prod_level/`

`cd linux_basic/notes/`

`cd ..` #listens to previous directory

`alias` #used to shorten long and frequently used commands

```
echo "hello linux"
alias ml="echo \ "hello linux \""
ml
```

## Variables

`env` #shows variables(variables) in the environment

`MY_NAME=Burak` #variable definition

`echo $MY_NAME` #print variable

`export MY_NAME` to export to #env

`env | grep MY_NAME`#To filter and fetch the variable

`unset MY_NAME` #to delete variable
`echo $MY_NAME`
`env | grep MY_NAME`

## Command Source with PATH Variable: which and types

`ls`

##### General format of commands :command -options arguments

`ls -l /etc`

`cd ls`

`echo $path` #points to directories.

`ls /usr/local/bin`

`pip -V` # to find out pip version

`/usr/local/bin/pip -V`

`nano simple_script.sh` # used as nano text editor

```
#!/bin/bash
echo "My name is Esat"
```

`chmod +x simple_script.sh` #chmod changes access privileges of a file

`./simple_script.sh`

`cd..`

`simple_script.sh`

`cd-notes/`

`pwd`

`/home/train/prod_level/linux_basic/notes/simple_script.sh`

`export PATH="/home/train/prod_level/linux_basic/notes:$PATH"`

`echo $PATH`

`simple_script.sh`

`cd..`

`simple_script.sh`

`type ls` # type : indicates which type of command (internal or external) it is
`type cd`
`type nano`

`which nano` #which : shows where (as path) the command was run from
`which echo`
'which cd'

## Regular Expressions with Grep-1

##### global regular expression print is used to filter using RegEx (regular expressions). format : grep option pattern files

<https://github.com/erkansirin78/datasets>

`cd prod_level/linux_basic/notes/`

`wget https://github.com/erkansirin78/datasets/master/words.txt` # wget link : downloads the file in the link

`ls`

`grep ^Mur words.txt` #looks for phrases starting with Mur in the file

`grep ^mur words.txt`

`grep -i ^mur words.txt` #searches with case insensitivity

`grep -in ^mur words.txt` # also shows which line the found expression is on.

`grep -in bird$words.txt` #searches in file for expressions ending with bird

`grep -in "k.s" words.txt`

`grep -in "k.ş$" words.txt`

`grep bak words.txt` #looks in file for expressions containing look

`grep 'look' words.txt`

`grep '\bak\' words.txt`

## Regular Expressions with Grep-2

`grep M[au]k words.txt` #containing a or u

`grep M[a-z]k words.txt` #containing any letter from a to z

`grep M[^ae]k words.txt` # [^ ] → contains all characters except the characters in square brackets

`grep M[ae]k words.txt`

`grep M[ü]k words.txt`

`wget https://github.com/erkansirin78/datasets/master/sample_tc_kimlik_names_numbers.txt`

`grep [111] sample_tc_identity_names_numbers.txt`

`grep [0-4] sample_tc_identity_names_numbers.txt`

`grep [^0-9] sample_tc_identity_names_numbers.txt`

`grep "[0-9]\{11\}" sample_tc_identity_names_numbers.txt`

`grep "[A-Za-z]\{6\}" sample_tc_identity_names_numbers.txt`

`grep "[0-9]\{11\}" sample_tc_identity_names_numbers.txt`

`grep -v "[0-9]\{11\}" sample_tc_identity_names_numbers.txt`

`grep -v "^2" sample_tc_identity_names_numbers.txt`

`grep 'Ak\|ş$ sample_tc_identity_names_numbers.txt`

`grep -E 'Ak|ş$ sample_tc_identity_names_numbers.txt`

##### grep --help : We can get information about the options of the grep command

`grep --help | grep '\-E'`

`grep -E 'Ak.ş$ sample_tc_identity_names_numbers.txt`

`grep -E 'Ak.n$ sample_tc_identity_names_numbers.txt`

`grep -E 'Ak.*n$ sample_tc_identity_names_numbers.txt`

`grep -E 'Ak.*ş$ sample_tc_identity_names_numbers.txt`

## Lesson6:Creating and Deleting Folders, Files: mkdir, touch, rm

`mkdir my_folder`

`ls`

`touch my_file` #creates a file

`ls`

`cd my_folder`

`pwd`

`rm my_file`

`rm my_folder`

`ls`

## File Copy and Move: cp, mv

`touch my_file1`

`mkdir my_folder1`

`cp my_file1 my_folder1/` #used to copy file/folder(cp filename destinationfolder)

`ls my_folder1`

`cat my_folder1/my_file1` prints the contents of the file #filename

`rm my_folder1/my_file1` is used to delete #rm

`ls my_folder1`

`mv my_file1 my_folder1/` #used to move file/folder(mv filename destinationfolder)

`mv my_folder1/my_file1 .`