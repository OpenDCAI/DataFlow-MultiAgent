"""JSON prompt references for the audited DataFlow reasoning operators.

Only fixed constructors are importable; user strings are never evaluated.
These definitions are embedded in standalone generated pipelines as well.
"""
# The one format-string prompt DataFlow ships; operators that take it accept a
# plain f-string template rather than a fixed prompt class.
FORMAT_PROMPT_CLASS = "FormatStrPrompt"

PROMPT_CLASSES = {
    FORMAT_PROMPT_CLASS: "dataflow.prompts.core_text",
    **{name: 'dataflow.prompts.reasoning.' + module
       for module, names in {
           'math': ('MathQuestionFilterPrompt', 'MathQuestionSynthesisPrompt', 'MathAnswerGeneratorPrompt'),
           'general': ('GeneralQuestionFilterPrompt', 'GeneralQuestionSynthesisPrompt', 'GeneralAnswerGeneratorPrompt'),
           'diy': ('DiyQuestionFilterPrompt', 'DiyQuestionSynthesisPrompt', 'DiyAnswerGeneratorPrompt'),
       }.items() for name in names},
}
OPERATOR_PROMPTS = {
    'ReasoningQuestionFilter': ('MathQuestionFilterPrompt', 'GeneralQuestionFilterPrompt', 'DiyQuestionFilterPrompt'),
    'ReasoningQuestionGenerator': ('MathQuestionSynthesisPrompt', 'GeneralQuestionSynthesisPrompt', 'DiyQuestionSynthesisPrompt'),
    'ReasoningAnswerGenerator': ('MathAnswerGeneratorPrompt', 'GeneralAnswerGeneratorPrompt', 'DiyAnswerGeneratorPrompt'),
}


def normalize_prompt(operator, value):
    if value is None:
        return None
    if isinstance(value, str):
        name, args = value.strip(), {}
        if name.endswith('()'):
            name = name[:-2]
    elif isinstance(value, dict) and set(value) <= {'$prompt', 'args'} and '$prompt' in value:
        name, args = value['$prompt'], value.get('args', {})
    else:
        raise ValueError(f'{operator}.prompt_template: use null or a {{$prompt: class_name, args: {{}}}} reference')
    if isinstance(name, str):
        for short, module in PROMPT_CLASSES.items():
            if name == module + '.' + short:
                name = short
                break
    if not isinstance(name, str) or name not in OPERATOR_PROMPTS[operator]:
        raise ValueError(f'{operator}.prompt_template: unsupported template {name!r}; allowed: {OPERATOR_PROMPTS[operator]}')
    if not isinstance(args, dict):
        raise ValueError(f'{operator}.prompt_template: args must be an object')
    if name.startswith('Diy'):
        if set(args) != {'prompt_template'} or not isinstance(args['prompt_template'], str) or not args['prompt_template'].strip():
            raise ValueError(f'{name} requires args.prompt_template with the intended prompt text')
    elif args:
        raise ValueError(f'{name} does not accept constructor arguments')
    return {'$prompt': name, 'args': dict(args)}


def instantiate_prompt(operator, value):
    import importlib
    ref = normalize_prompt(operator, value)
    if ref is None:
        return None
    name = ref['$prompt']
    return getattr(importlib.import_module(PROMPT_CLASSES[name]), name)(**ref['args'])
