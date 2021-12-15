# data-anonymizer
Python tool to perform data anonymization on a MySQL database


"anonymize_instructions.txt" describes what database, tables and what columns in the data to perform data anonymization on.

Line 1: [database name]
Line 2-beyond: [table], [col name] [categorical or numerical] [faker provider function to use]
