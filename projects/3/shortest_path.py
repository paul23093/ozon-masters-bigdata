def shortest_path(v_from, v_to, df, max_path_length=10):
    from pyspark.sql.functions import col, concat, lit
    schema = StructType(fields=[
        StructField(\"c1\", IntegerType()),
        StructField(\"c0\", IntegerType())
    ])
    graph = raw_graph.distinct().cache()
    temp_df_c = graph.filter('c0 = {v_from}')
    i = 1
    output_columns = ['c0', lit(','), 'c1']
    while i < max_path_length:
        graph = graph.select(col(f'c{i-1}').alias(f'c{i}'), col(f'c{i}').alias(f'c{i+1}'))
        temp_df = temp_df_c
        temp_df_c.unpersist()
        temp_df_c = temp_df.join(graph, f'c{i}', 'inner').cache()
        temp_df_c.show()
        tmp = temp_df_c.filter(f'c{i+1} = {v_to}').count()
        output_columns.append(lit(','))
        output_columns.append(f'c{i+1}')
        if tmp > 0:
            break
        i += 1
    (
    temp_df_c
    .filter(f'c{i+1} = 34')
    .select(
        concat(*output_columns).alias('path')
    )
    .show(20, False)
    )
    temp_df_c.select(\"path\").write.mode(\"overwrite\").text(\"hw3_output\")