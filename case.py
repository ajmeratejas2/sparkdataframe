def num_to_string(x):
    match x :
        case 0 :
            return "zero"
        case 1 :
            return "one"
        case 2 :
            return "two"
        case default :
            return "smoething"
if __name__ == "__main__" :
    x=0
    y=num_to_string(x)
    print(y)