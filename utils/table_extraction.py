import json
from unstructured.partition.pdf import partition_pdf
import pandas as pd

def extract_tables_from_pdf(filename, strategy='hi_res'):
    """
    Extracts all tables from the given PDF file.

    Args:
        filename (str): Path to the PDF file.
        strategy (str): Strategy for table extraction ('hi_res' or other supported strategies).

    Returns:
        Tuple[List[pd.DataFrame], List[str]]: A tuple containing a list of DataFrames for each table 
        and a list of HTML representations of the tables.
    """
    try:
        # Check if poppler is installed
        import subprocess
        try:
            subprocess.run(['pdfinfo', '-v'], capture_output=True, check=True)
        except (subprocess.SubprocessError, FileNotFoundError):
            raise RuntimeError(
                "Poppler is not installed or not in PATH. "
                "Please install poppler-utils:\n"
                "- Windows: choco install poppler\n"
                "- MacOS: brew install poppler\n"
                "- Linux: sudo apt-get install poppler-utils"
            )
            
        # Check if tesseract is installed
        try:
            subprocess.run(['tesseract', '--version'], capture_output=True, check=True)
        except (subprocess.SubprocessError, FileNotFoundError):
            raise RuntimeError(
                "Tesseract is not installed or not in PATH. "
                "Please install tesseract-ocr:\n"
                "- Windows: choco install tesseract\n"
                "- MacOS: brew install tesseract\n"
                "- Linux: sudo apt-get install tesseract-ocr"
            )
        
        # Add parameters to control processing
        elements = partition_pdf(
            filename=filename,
            infer_table_structure=True,
            strategy=strategy,
            include_page_breaks=False,  # Skip page breaks for faster processing
            include_metadata=True,
            max_partition=20,  # Limit partitions for large documents
        )

        # Use list comprehension for better performance
        tables = [el for el in elements if el.category == "Table"]
        
        if not tables:
            print("No tables found in the PDF.")
            return [], []

        dfs = []
        tables_html = []
        
        # Process tables in batches
        for idx, table in enumerate(tables, start=1):
            try:
                html = table.metadata.text_as_html
                tables_html.append(html)
                
                # Use error handling for pd.read_html
                df_list = pd.read_html(html, flavor='bs4')  # Specify parser
                if df_list:
                    dfs.append(df_list[0])
                    print(f"Table {idx} extracted successfully.")
                else:
                    print(f"No data found in Table {idx}")
                    
            except Exception as e:
                print(f"Error processing Table {idx}: {str(e)}")
                continue

        return dfs, tables_html

    except Exception as e:
        print(f"⚠️ Error extracting tables: {str(e)}")
        return [], []
