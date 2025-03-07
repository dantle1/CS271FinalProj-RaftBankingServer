def process_file(input_filename="input.txt"):
    # Define the output files
    output_files = {
        "c1": "c1.txt",
        "c2": "c2.txt",
        "c3": "c3.txt",
        "crossShard": "crossShard.txt"
    }

    # Open all output files in append mode
    file_handles = {key: open(filename, "w") for key, filename in output_files.items()}

    try:
        # Read input file line by line
        with open(input_filename, "r") as infile:
            for line_number, line in enumerate(infile, start=1):
                parts = line.strip().split()

                # Ensure the line has at least two elements
                if len(parts) < 2:
                    continue

                # Extract <a> and <b> as integers
                try:
                    a, b = int(parts[0]), int(parts[1])
                except ValueError:
                    continue  # Skip lines where conversion to integer fails

                # Determine the category
                if 1 <= a <= 1000 and 1 <= b <= 1000:
                    category = "c1"
                elif 1001 <= a <= 2000 and 1001 <= b <= 2000:
                    category = "c2"
                elif 2001 <= a <= 3000 and 2001 <= b <= 3000:
                    category = "c3"
                else:
                    category = "crossShard"

                # Write to the corresponding file with line number prepended
                file_handles[category].write(f"{line}")

    finally:
        # Close all file handles
        for handle in file_handles.values():
            handle.close()

if __name__ == "__main__":
    process_file()