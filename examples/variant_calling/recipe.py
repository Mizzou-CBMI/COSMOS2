import os
from cosmos.api import Cosmos, one2one, many2one, add_execution_args
import argparse
import subprocess as sp
import tools


def variant_call(execution, max_attempts, max_cores, bam_path, target_bed_path, max_complex_gap):
    """
    Bioinformatics variant calling workflow
    """
    contigs = sp.check_output("cat %s |cut -f1|uniq" % target_bed_path, shell=True).strip().split("\n")

    freebayes_tasks = []
    for contig in contigs:
        bed_task = execution.add_task(tools.filter_bed_by_contig, tags=dict(in_bam=bam_path, in_bed=target_bed_path, contig=contig), out_dir='work/{contig}')
        freebayes_task = execution.add_task(tools.freebayes, tags=dict(max_complex_gap=max_complex_gap), parents=bed_task, out_dir='work/{contig}')
        freebayes_tasks.append(freebayes_task)

    merge_vcf_tasks = many2one(tools.vcf_concat_parts, parents=freebayes_tasks)

    execution.run(max_attempts=max_attempts, max_cores=max_cores)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('bam_path')
    p.add_argument('target_bed_path')
    p.add_argument('--max_complex_gap', type=int, default=2)
    add_execution_args(p)
    args = p.parse_args()

    cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)))
    cosmos.initdb()
    execution = cosmos.start(name=args.name, output_dir='../analysis_output/variant_calling',
                             restart=args.restart, skip_confirm=args.skip_confirm)

    variant_call(execution, args.max_attempts, args.max_cores, args.bam_path, args.target_bed_path, args.max_complex_gap)
