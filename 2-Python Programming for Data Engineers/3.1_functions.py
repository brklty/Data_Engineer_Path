#################################################

# Functions

#################################################


print("a")

print("a", "b")

print("a", "b", sep="--")


# - function definition

def calculate(a):
    print(a * 2)


calculate(4)


# - define a function with two arguments/parameters

def summer(arg1, arg2):
    print(arg1 + arg2)


summer(3, 7)

summer(arg2=8, arg1=3)


# - Docstring

def summer(arg1, arg2):
    # -- google docstring
    """
    Sum of two numbers.
    Args:
        arg1: int, float
        arg2: int, float

    Returns:
        int, float

    """

    # -- numpy docstring
    """

    Parameters
    ----------
    arg1
    arg2

    Returns
    -------

    """

    print(arg1 + arg2)


help(summer)

summer(1, 4)

# - Statement/Body Section of Functions

"""
def func_name(arguments):
    statement(func body)
"""


def say_hi(string):
    print("Hello " + string)
    print("hi " + string)
    print("hallo " + string)


say_hi("George")


def multiplication(a, b):
    c = a * b
    print(c)


multiplication(10, 9)

# -- function to store the entered values in a list.


list_store = []


def add_element(a, b):
    c = a * b
    list_store.append(c)
    print(list_store)


add_element(1, 10)
add_element(2, 20)
add_element(3, 30)


# - Default Parameters/Arguments


def divide(a, b=1):
    print(a / b)


divide(10)


def divide1(a, b):
    print(a / b)


divide1(10)


def say_hi(string="hallo"):
    print(string)
    print("Hi")
    print("Hello")


say_hi()


# - return


def calculate(warm, moisture, charge):
    return (warm + moisture) / charge


calculate(98, 12, 78) + 10


def calculate(warm, moisture, charge):
    warm = warm * 2
    moisture = moisture * 2
    charge = charge * 2
    output = (warm + moisture) / charge

    return warm, moisture, charge, output


type(calculate(98, 12, 78))

warm, moisture, charge, output = calculate(98, 12, 78)


# - Calling a Function from within a Function


def calculate(warm, moisture, charge):
    return int((warm + moisture) / charge)


calculate(98, 12, 78)


def standardization(a, p):
    return a * 10 / 100 * p * p


standardization(45, 1)


def all_calculate(warm, moisture, charge, p):
    a = calculate(warm, moisture, charge)
    b = standardization(a, p)
    print(b * 10)


all_calculate(98, 12, 78, 2)

# -local/global variables


list_store = [1, 2]  # global


def add_element(a, b):
    c = a * b  # local
    list_store.append(c)
    print(list_store)


add_element(1, 9)


