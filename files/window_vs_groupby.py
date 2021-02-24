from pyspark.sql import Window
from pyspark.sql import functions as fn

### Comparison between window function and groupby

# products table
# +-------+--------+----------+----------+
# |prod_id|prod_cat|prod_brand|prod_value|
# +-------+--------+----------+----------+
# |      4|keyboard|  logitech|     49.99|
# |      5|   mouse|  logitech|     29.99|
# |      1|   mouse| microsoft|     39.99|
# |      2|   mouse| microsoft|     59.99|
# |      3|keyboard| microsoft|     59.99|
# +-------+--------+----------+----------+

### Objective: add a column containing the average product value per brand

### Method 1:  Group by
step_1 = products.groupBy('prod_brand')\
        .agg(fn.round(fn.avg('prod_value'))) 
# there is a first shuffle here to gather dataframe rows with the same
# prdo_brand together during the groupBy

# +----------+-----------------+
# |prod_brand|  avg(prod_value)|
# +----------+-----------------+
# |microsoft  |           53.32|
# | logitech  |           39.99|
# +--------+-------------------+

# The groupBy allows to compute the desired average, but it results in a 
# smaller number of rows
        
# Solution: do a join
products.join(step_1, 'prod_brand').show()
# Drawback: this join causes a second shuffle

# Results:
# +-------+--------+----------+----------+---------------+
# |prod_id|prod_cat|prod_brand|prod_value|avg_brand_value|
# +-------+--------+----------+----------+---------------+
# |      4|keyboard|  logitech|     49.99|          39.99|
# |      5|   mouse|  logitech|     29.99|          39.99|
# |      1|   mouse| microsoft|     39.99|          53.32|
# |      2|   mouse| microsoft|     59.99|          53.32|
# |      3|keyboard| microsoft|     59.99|          53.32|
# +-------+--------+----------+----------+---------------+


# Window function
# First, we create the Window definition
window = Window.partitionBy('prod_brand')
# Then, we can use "over" to aggregate on this window
avg = fn.avg('prod_value').over(window)
# Finally, we can use this as usual
products.withColumn('avg_brand_value', fn.round(avg, 2)).show()
# Same result, but with only one shuffle resulting from the partitionBy defined
# in the window -> less shuffles = faster execution

# +-------+--------+----------+----------+---------------+
# |prod_id|prod_cat|prod_brand|prod_value|avg_brand_value|
# +-------+--------+----------+----------+---------------+
# |      4|keyboard|  logitech|     49.99|          39.99|
# |      5|   mouse|  logitech|     29.99|          39.99|
# |      1|   mouse| microsoft|     39.99|          53.32|
# |      2|   mouse| microsoft|     59.99|          53.32|
# |      3|keyboard| microsoft|     59.99|          53.32|
# +-------+--------+----------+----------+---------------+
