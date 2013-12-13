options <- commandArgs(trailingOnly = TRUE)

input_file_path = options[1]
output_file_path = options[2]

library(ggplot2)
df = read.csv(input_file_path)

png(file=output_file_path, width=800, height=600,res=100)
Stage = df$stage
qplot(df$avg_rss_mem/1024/1024, df$wall_time/60, colour = Stage, xlab="Memory (GB)",ylab="Time (min)",main="Wall Time vs Average Memory")
dev.off()