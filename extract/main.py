from database import Extract

def main():

    print("Enter the regions file path, after given space: \n")
    print("*"*50)
    # regions_file_path = input()
    regions_file_path = r"C:\Users\91700\Downloads\order_region_b(in).csv  C:\Users\91700\Downloads\order_region_a(in).csv"

    Extract(regions_file_path.split())

if __name__ == "__main__":
    main()