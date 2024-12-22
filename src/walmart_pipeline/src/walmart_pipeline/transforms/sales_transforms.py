import apache_beam as beam
import logging

class SalesTransform(beam.PTransform):

    """Apache Beam PTransform for processing Walmart sales data.
    
    This transform filters sales records, groups them by month, and calculates
    average weekly sales for each month.
    """
     
    def expand(self, pcoll):
        return (pcoll 
                | "Filter Sales" >> beam.Filter(self.filter_sales)
                | "Group By Month" >> beam.GroupBy(lambda x: x['Month'])
                | "Calculate Average" >> beam.Map(self.calculate_average))
    
    @staticmethod
    def filter_sales(record):
        try:
            return float(record['Weekly_Sales']) > 10000
        except (ValueError, KeyError) as e:
            logging.error(f"Error filtering sales: {e}")
            return False
    
    @staticmethod
    def calculate_average(group):
        month, records = group
        sales = [float(record['Weekly_Sales']) for record in records]
        avg_sales = round(sum(sales) / len(sales), 2) if sales else 0.0
        
        return {
            'Month': month,
            'Average_Weekly_Sales': avg_sales
        }