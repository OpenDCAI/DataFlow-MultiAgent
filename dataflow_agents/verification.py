"""Deterministic execution/field checks; no model or content-quality judgement."""


def verify_fields(spec, runtime, output, *, output_exists):
    issues = []
    if runtime.get('status') != 'passed' or not runtime.get('compile') or not runtime.get('executed'):
        issues.append(runtime.get('error') or 'Pipeline 未成功完成编译和执行')
    if not output_exists:
        issues.append('Pipeline 没有生成输出文件')
    expected = set(spec['final_keys'])
    if not expected:
        issues.append('Pipeline 未声明最终输出字段')
    # The executor reports the dataframe schema even for an empty output.
    missing = expected - set(runtime.get('fields', []))
    if missing:
        issues.append('执行结果缺少字段：' + ', '.join(sorted(missing)))
    if runtime.get('rows') != len(output):
        issues.append('输出文件行数与执行报告不一致')
    for index, row in enumerate(output, 1):
        if not isinstance(row, dict):
            issues.append(f'第 {index} 行不是 JSON 对象')
        elif set(row) != expected:
            missing = expected - set(row)
            extra = set(row) - expected
            issues.append(f'第 {index} 行字段不匹配；缺少：{sorted(missing)}，多出：{sorted(extra)}')
        if len(issues) >= 20:
            break  # Enough evidence to reject; bound the repair prompt size.
    return {'verdict': 'fail' if issues else 'pass',
            'reason': '执行或字段检查未通过' if issues else 'Pipeline 执行成功，输出字段检查通过；未进行内容语义评价',
            'issues': issues}
