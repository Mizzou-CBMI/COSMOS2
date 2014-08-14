record_cols = ['cdiff','chrom','start','stop','ref','alleles', 'var_type', 'qual','set', 'filter','cropwarn','MergeStatus', 'cg_class']
classes = ['pos-identical','allele-identical','filter-consistent','cropwarn-consistent']

def compare_readers(A_name, a_reader, B_name, b_reader, a_path):
    def record_info(cdiff_name, r):
        return [ cdiff_name, r.chrom, r.start, r.stop, r.ref, r.genos[0].alleles, r.var_type.name, r.qual, r.info.get('set',None), r.filter, r.info.get('CROPWARN',[]), r.info.get('MergeStatus',None), cgclass2meta(r.info['locusDiffClassification']) ]
    
    for (chrom, start, stop), group in it.groupby(sorted(it.chain(add_side(a_reader, A_name), add_side(b_reader, B_name)),key=key), key):
        group = list(group)
        if len(group) == 1:
            (record, side) = group[0]
            yield record_info(side, record) + [ False, False, False, False ]
        else:
            if len(group) == 2:
                (a,side), (b,side2) = group[0:2]
                assert side == A_name
                assert side2 == B_name
                (x,y,z) = comp(a,b)
                yield record_info(A_name, a) + [True, x, y, z]
                yield record_info(B_name, b) + [True, x, y, z]
            else:
                print '>2 records with the same pos; a_path: %s:\n%s' % (a_path, "\n".join([ str(record_info(side, r)) for r, side in group ]))
                for record, side in group:
                    yield record_info(side, record) + [ False, False, False, False ]
    