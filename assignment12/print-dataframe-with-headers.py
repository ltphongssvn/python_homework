import pandas as pd
import os

class DFPlus(pd.DataFrame):
    """
    An enhanced version of pandas DataFrame with additional display capabilities.
    
    This class demonstrates the power of inheritance in Python - we get all
    the functionality of DataFrame while adding our own custom methods.
    Think of this as a DataFrame with superpowers for better data viewing.
    """
    
    @property
    def _constructor(self):
        """
        This special property tells pandas what class to use when creating
        new objects from operations on this DataFrame.
        
        Without this, operations like df.head() or df[df > 0] would return
        regular DataFrame objects instead of DFPlus objects, breaking our
        enhanced functionality.
        
        Think of this as ensuring that when someone makes a copy of your
        specialized car, they get all your custom features, not just a basic car.
        """
        return DFPlus
    
    @classmethod
    def from_csv(cls, filepath, **kwargs):
        """
        Class method to create a DFPlus instance directly from a CSV file.
        
        This solves a fundamental problem: pd.read_csv() creates DataFrame objects,
        but we need DFPlus objects. This method acts as a bridge, reading the CSV
        with pandas' proven CSV parser, then converting the result to our enhanced type.
        
        Args:
            filepath (str): Path to the CSV file to read
            **kwargs: Any additional arguments to pass to pd.read_csv()
                     (like sep=',', header=0, etc.)
        
        Returns:
            DFPlus: An instance of our enhanced DataFrame loaded with CSV data
            
        Example:
            dfp = DFPlus.from_csv("../csv/products.csv")
            dfp = DFPlus.from_csv("data.csv", sep=';', encoding='utf-8')
        """
        # First, check if the file exists to provide helpful error messages
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"CSV file not found: {filepath}")
        
        # Use pandas' robust CSV reading capabilities
        df = pd.read_csv(filepath, **kwargs)
        
        # Convert the regular DataFrame to our enhanced DFPlus type
        # cls refers to the DFPlus class, so cls(df) creates a DFPlus instance
        return cls(df)
    
    def print_with_headers(self, rows_per_section=10):
        """
        Print the DataFrame with column headers repeated every few rows.
        
        This method solves a real problem that data scientists face: when you
        print a large DataFrame, the column headers scroll off the screen,
        making it hard to interpret the data. This method keeps headers visible
        throughout the output.
        
        Args:
            rows_per_section (int): How many data rows to show before repeating headers
                                   Default is 10, but you can customize this
        """
        # Handle edge case: empty DataFrame
        if len(self) == 0:
            print("DataFrame is empty - no data to display")
            return
        
        print(f"Enhanced DataFrame Display ({len(self)} rows × {len(self.columns)} columns)")
        print("=" * 80)
        
        # Calculate how many sections we'll need to display the entire DataFrame
        total_rows = len(self)
        sections_needed = (total_rows + rows_per_section - 1) // rows_per_section  # Ceiling division
        
        # Process the DataFrame in chunks, showing headers before each chunk
        for section_num in range(sections_needed):
            # Calculate the row range for this section
            start_row = section_num * rows_per_section
            end_row = min(start_row + rows_per_section, total_rows)
            
            # Show section information for large datasets
            if sections_needed > 1:
                print(f"\n--- Section {section_num + 1} of {sections_needed} (rows {start_row}-{end_row-1}) ---")
            
            # Get the slice of data for this section using iloc (integer location indexing)
            # This is more reliable than using regular slicing because it works with any index type
            section_data = self.iloc[start_row:end_row]
            
            # Print the section with its headers
            # pandas automatically includes headers when you print a DataFrame
            print(section_data)
            
            # Add visual separation between sections (except for the last one)
            if section_num < sections_needed - 1:
                print("-" * 60)
        
        print("=" * 80)
        print(f"Display complete: {total_rows} rows shown in {sections_needed} section(s)")


def demonstrate_dfplus_capabilities():
    """
    Comprehensive demonstration of our DFPlus class capabilities.
    This shows both the inherited DataFrame functionality and our custom enhancements.
    """
    print("🐼 DFPlus Demonstration: Enhanced DataFrame in Action 🐼")
    print("=" * 70)
    
    # Test our from_csv class method
    csv_path = "../csv/products.csv"
    
    try:
        # Create our enhanced DataFrame from CSV
        print(f"Loading data from {csv_path}...")
        dfp = DFPlus.from_csv(csv_path)
        
        print(f"✅ Successfully loaded {len(dfp)} rows and {len(dfp.columns)} columns")
        print(f"Data type: {type(dfp).__name__}")  # Should show "DFPlus"
        
        # Show that we inherited all DataFrame capabilities
        print(f"\n📊 Basic DataFrame Information:")
        print(f"Shape: {dfp.shape}")
        print(f"Columns: {list(dfp.columns)}")
        print(f"Data types preview: {dfp.dtypes.to_dict()}")
        
        # Demonstrate that operations preserve our DFPlus type
        print(f"\n🔬 Testing Inheritance Behavior:")
        head_result = dfp.head(3)
        print(f"dfp.head(3) returns type: {type(head_result).__name__}")
        print("This proves our _constructor property is working!")
        
        # Show a preview before the enhanced display
        print(f"\n👀 Standard DataFrame Preview (first 5 rows):")
        print(dfp.head())
        
        # Now demonstrate our custom enhancement
        print(f"\n🚀 Using Our Custom Enhanced Display Method:")
        print("This will show headers every 10 rows, making large datasets much more readable!")
        dfp.print_with_headers(rows_per_section=8)  # Use 8 rows per section for demo
        
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        print("Make sure you have ../csv/products.csv in your project structure")
        print("For demonstration purposes, let's create a sample dataset...")
        
        # Create sample data if the CSV doesn't exist
        sample_data = {
            'Product': [f'Product_{i}' for i in range(1, 26)],
            'Price': [10.99 + i * 2.5 for i in range(25)],
            'Category': ['Electronics', 'Books', 'Clothing'] * 8 + ['Electronics'],
            'Stock': [100 - i * 3 for i in range(25)]
        }
        
        dfp = DFPlus(sample_data)
        print(f"✅ Created sample DFPlus with {len(dfp)} rows")
        print("Testing our enhanced display with sample data:")
        dfp.print_with_headers(rows_per_section=7)


def main():
    """
    Main function that orchestrates our DataFrame enhancement demonstration.
    This shows the complete workflow from CSV loading to enhanced display.
    """
    print("🎓 Learning Advanced Python: Class Inheritance with pandas 🎓")
    print("This program demonstrates how to extend existing classes with custom functionality")
    print("while preserving all their original capabilities.\n")
    
    # Run our comprehensive demonstration
    demonstrate_dfplus_capabilities()
    
    print(f"\n📚 Key Learning Points:")
    print("• Class inheritance allows us to extend existing functionality")
    print("• The @property decorator can control how pandas creates new objects")
    print("• @classmethod provides alternative ways to construct objects")
    print("• iloc indexing gives us precise control over DataFrame slices")
    print("• Custom methods can solve real-world data analysis problems")
    
    print(f"\n🎯 Professional Applications:")
    print("• Custom DataFrame classes for domain-specific data analysis")
    print("• Enhanced display methods for different types of reports")
    print("• Specialized data loading and validation workflows")
    print("• Integration of business logic with data manipulation tools")


# Execute our demonstration when the script is run directly
if __name__ == "__main__":
    main()