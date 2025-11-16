import sys

def find_query_offsets(filename, query):
    """Find all byte offsets where query exists in the file."""
    offsets = []
    
    try:
        with open(filename, 'rb') as f:
            content = f.read()
            query_bytes = query.encode('utf-8')
            
            start = 0
            while True:
                pos = content.find(query_bytes, start)
                if pos == -1:
                    break
                offsets.append(pos)
                start = pos + 1
                
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    return offsets

def main():
    # Ask for filename
    filename = input("Enter filename: ").strip()
    
    # Ask for query
    query = input("Enter query string: ").strip()
    
    # Find offsets
    offsets = find_query_offsets(filename, query)
    
    # Output results
    if offsets:
        print(f"\nFound {len(offsets)} occurrence(s) of '{query}':")
        print(f"  Offsets = {offsets}")
    else:
        print(f"\nQuery '{query}' not found in file.")

if __name__ == "__main__":
    main()