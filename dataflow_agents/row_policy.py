"""Conservative handling of explicit no-row-filtering instructions."""
import re


def preserve_all_rows(text, constraints=None):
    constraints = constraints or {}
    if any(constraints.get(key) is True for key in ('preserve_all_rows', 'preserve_rows', 'forbid_row_filters')):
        return True
    if constraints.get('allow_filtering') is False:
        return True
    patterns = (
        r'(?:保留|保持)(?:全部|所有|每一条|每条)(?:原始|输入|数据)?(?:记录|数据行|行)',
        r'(?:不|不要|不得|禁止)(?:删除|过滤|丢弃|移除)(?:任何|任意|所有|全部|原始)?(?:数据)?(?:记录|行)',
        r'(?:不要|不得|禁止)(?:编写|写|使用|引入|添加)?(?:任何)?过滤算子',
        r'\b(?:keep|preserve|retain)\s+(?:all|every)\s+(?:(?:original|input|source)\s+)?(?:rows?|records?)\b',
        r'\b(?:no|without)\s+(?:row\s+)?filtering\b',
        r'\b(?:do\s+not|don.t|never)\s+(?:filter|delete|drop|remove)\s+(?:any\s+)?(?:rows?|records?)\b',
        r'\b(?:do\s+not|don.t|never)\s+(?:use|add|generate)\s+(?:any\s+)?(?:row\s+)?filter(?:s|ing\s+operators?)?\b',
    )
    return any(re.search(pattern, text, re.IGNORECASE) for pattern in patterns)


def row_filter_errors(bindings, catalog, required):
    if not required:
        return []
    index = {op['name']: op for op in catalog}
    errors = []
    for binding in bindings:
        name = binding.get('operator', '')
        category = index.get(name, {}).get('category', '').lower().split('.')
        if name.lower().endswith('filter') or 'filter' in category or 'filters' in category:
            errors.append(f"{binding['step_id']}: 用户明确要求保留全部记录或禁止过滤，不能使用行过滤算子 {name}；"
                          '请使用保留记录的转换或标签生成实现，不要删除行。')
    return errors


def row_loss_error(required, input_rows, output_rows):
    if required and output_rows < input_rows:
        return f'用户要求保留全部记录，但输入 {input_rows} 行、输出 {output_rows} 行；请移除删除或过滤行的逻辑。'
    return None
