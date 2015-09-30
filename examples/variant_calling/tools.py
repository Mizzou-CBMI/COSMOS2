from cosmos.api import find, out_dir, args
import configparser

settings = configparser.ConfigParser()
settings.read('settings.conf')


def filter_bed_by_contig(contig,
                         drm='local',
                         in_bed=find('bed$'),
                         out_bed=out_dir('target.bed')):
    return r"""
        grep -P "^{contig}\t" {in_bed} > {out_bed}
    """.format(s=settings, **locals())


def freebayes(reference_fasta=settings['ref']['reference_fasta'],
              max_complex_gap=2,
              no_complex=True,
              in_target_bed=find('bed$'), in_bam=find('bam$'),
              out_vcf=out_dir('variants.vcf')):
    return r"""
        {s[opt][freebayes]} -f {reference_fasta} \
        --vcf {out_vcf} \
        --targets {in_target_bed} \
        {args} \
        -m 30 -q 10 -R 0 -S 0 -F 0.1 \
        {in_bam}
    """.format(s=settings,
               args=args(('--max-complex-gap', max_complex_gap),
                         ('--no-complex', no_complex)),
               **locals())


def vcf_concat_parts(in_vcfs=find('vcf$', n='>0'), out_vcf=out_dir('freebayes.vcf')):
    return r"""
        {s[opt][vcf_concat_parts]} {vcfs} > {out_vcf}
    """.format(s=settings, vcfs=' '.join(in_vcfs), **locals())
