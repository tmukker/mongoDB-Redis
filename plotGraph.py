import csv
import matplotlib.pyplot as plt
import seaborn as sns
import statistics

# Read response times from CSV file
file_path_case_1 = "test_case_1_results.csv"  # Change to your file path
file_path_case_2 = "test_case_2_results.csv"  # Change to your file path


def plot_histogram(file_path, label):
    with open(file_path, mode="r") as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        response_times = [float(row[0]) for row in reader]

    # Plot histogram
    sns.histplot(response_times, kde=True, label=label)

    # Add labels and title
    plt.xlabel('Response Time (seconds)')
    plt.ylabel('Frequency')
    plt.title(f'Histogram of Response Times - {label}')

    # Calculate and plot average response time
    avg_response_time = statistics.mean(response_times)
    plt.axvline(avg_response_time, color='red', linestyle='dashed', linewidth=2, label=f'Avg {avg_response_time:.2f}')

    # Add legend
    plt.legend()

    # Show the plot
    plt.show()


# Plot for test case 1
plot_histogram(file_path_case_1, 'Test Case 1')

# Plot for test case 2
plot_histogram(file_path_case_2, 'Test Case 2')
