import os
from cosmos.api import Cosmos, one2one, many2one, add_execution_args, pop_execution_args
import argparse
import subprocess as sp
import tools


def variant_call(execution, bam_path, target_bed_path, max_complex_gap):
    """
    Bioinformatics variant calling workflow
    """
    contigs = sp.check_output("cat %s |cut -f1|uniq" % target_bed_path, shell=True).strip().split("\n")

    bed_tasks = [execution.add_task(tools.filter_bed_by_contig, tags=dict(in_bam=bam_path, in_bed=target_bed_path, contig=contig), out_dir='work/{contig}')
                 for contig in contigs ]

    freebayes_tasks = one2one(tools.freebayes, bed_tasks, dict(max_complex_gap=max_complex_gap))

    merge_vcf_tasks = many2one(tools.vcf_concat_parts, freebayes_tasks)

    execution.run()


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('bam_path')
    p.add_argument('target_bed_path')
    p.add_argument('--max_complex_gap', type=int, default=2)
    add_execution_args(p)
    start_kwargs, variant_call_args = pop_execution_args(vars(p.parse_args()))

    cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)))
    cosmos.initdb()
    execution = cosmos.start(output_dir='../analysis_output/variant_calling', **start_kwargs)

    variant_call(execution, **variant_call_args)
