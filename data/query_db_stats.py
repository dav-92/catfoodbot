import os
import sys

# Ensure we use the correct database URL inside the container
if "DATABASE_URL" not in os.environ:
    os.environ["DATABASE_URL"] = "sqlite:///data/catfood.db"

try:
    from database import get_product_statistics
except ImportError:
    sys.path.append(os.getcwd())
    from database import get_product_statistics

def main():
    try:
        stats = get_product_statistics()
        
        # Write to a file in the data volume so it persists and is accessible from host
        output_path = '/app/data/full_stats.txt'
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(f"Total Products: {sum(s['count'] for s in stats)}\n\n")
            f.write(f"{'Site':<15} | {'Brand':<30} | {'Count':<5}\n")
            f.write("-" * 55 + "\n")
            
            for s in stats:
                f.write(f"{s['site']:<15} | {s['brand']:<30} | {s['count']:<5}\n")
                
        print(f"Statistics written to {output_path}")
        
    except Exception as e:
        print(f"Error querying statistics: {e}")

if __name__ == "__main__":
    main()
