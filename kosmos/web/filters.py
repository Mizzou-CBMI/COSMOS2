import datetime, re

def add_filters(bprint):
    @bprint.add_app_template_filter
    def to_thumb(b):
        return 'yes' if b else 'no'

    def format_time(amount, type="seconds"):
        if amount is None or amount == '':
            return ''
        if type == 'minutes':
            amount = amount * 60
        return datetime.timedelta(seconds=int(amount))


    @bprint.add_app_template_filter
    def format_resource_usage(field_name, val, help_txt):
        if val is None:
            return ''
        elif re.search(r"\(Kb\)", help_txt):
            if val == 0: return '0'
            return "{0} ({1})".format(val, format_memory_kb(val))
        elif re.search(r"time", field_name):
            return "{1}".format(val, format_time(val))
        elif field_name == 'percent_cpu':
            return "{0}%".format(val)
        elif type(val) in [int, long]:
            return intWithCommas(val)
        return str(val)


    def intWithCommas(x):
        if x == None: return ''
        if type(x) not in [type(0), type(0L)]:
            raise TypeError("Parameter must be an integer.")
        if x < 0:
            return '-' + intWithCommas(-x)
        result = ''
        while x >= 1000:
            x, r = divmod(x, 1000)
            result = ",%03d%s" % (r, result)
        return "%d%s" % (x, result)


    def format_memory_kb(kb):
        """converts kb to human readible"""
        if kb is None:
            return ''
        mb = kb / 1024.0
        gb = mb / 1024.0
        if gb > 1:
            return "%sGB" % round(gb, 1)
        else:
            return "%sMB" % round(mb, 1)


    def format_memory_mb(mb):
        """converts mb to human readible"""
        return format_memory_kb(mb * 1024.0) if mb else ""