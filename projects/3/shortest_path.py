{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 17,
   "metadata": {},
   "outputs": [
    {
     "ename": "SyntaxError",
     "evalue": "invalid syntax (<ipython-input-17-e8a260e6f434>, line 7)",
     "output_type": "error",
     "traceback": [
      "\u001b[0;36m  File \u001b[0;32m\"<ipython-input-17-e8a260e6f434>\"\u001b[0;36m, line \u001b[0;32m7\u001b[0m\n\u001b[0;31m    def min_distance((source1, dist1, source2, dist2):\u001b[0m\n\u001b[0m                     ^\u001b[0m\n\u001b[0;31mSyntaxError\u001b[0m\u001b[0;31m:\u001b[0m invalid syntax\n"
     ]
    }
   ],
   "source": [
    "def shortest_path(v_from, v_to, df, max_path_length=10):\n",
    "    schema = StructType(fields=[\n",
    "        StructField(\"c1\", IntegerType()),\n",
    "        StructField(\"c0\", IntegerType())\n",
    "    ])\n",
    "    graph = raw_graph.distinct().cache()\n",
    "    temp_df_c = graph.filter('c0 = {v_from}')\n",
    "    \n",
    "    i = 1\n",
    "\n",
    "    output_columns = ['c0', lit(','), 'c1']\n",
    "    while i < max_path_length:\n",
    "        graph = graph.select(col(f'c{i-1}').alias(f'c{i}'), col(f'c{i}').alias(f'c{i+1}'))\n",
    "        temp_df = temp_df_c\n",
    "        temp_df_c.unpersist()\n",
    "        temp_df_c = temp_df.join(graph, f'c{i}', 'inner').cache()\n",
    "        temp_df_c.show()\n",
    "        tmp = temp_df_c.filter(f'c{i+1} = {v_to}').count()\n",
    "        output_columns.append(lit(','))\n",
    "        output_columns.append(f'c{i+1}')\n",
    "        if tmp > 0:\n",
    "            break\n",
    "        i += 1\n",
    "        \n",
    "    (\n",
    "    temp_df_c\n",
    "    .filter(f'c{i+1} = 34')\n",
    "    .select(\n",
    "        concat(*output_columns).alias('path')\n",
    "    )\n",
    "    .show(20, False)\n",
    "    )\n",
    "    temp_df_c.select(\"path\").write.mode(\"overwrite\").text(\"hw3_output\")"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "dsenv",
   "language": "python",
   "name": "dsenv"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.4"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
