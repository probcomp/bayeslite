import bayeslite.metamodels.crosscat_generator_schema as cgschema


def test_parses_a_column():
    # The trailing [] in schema is what you get if there's a trailing comma in
    # the expression.
    schema = [['x', 'NUMERICAL'], []]
    parsed = cgschema.parse(schema, False)
    expected = cgschema.GeneratorSchema(
        guess=False, subsample=False, columns=[('x', 'NUMERICAL')],
        dep_constraints=[])
    assert parsed == expected


def test_parses_dep_constraints():
    schema = [
        ['a', 'NUMERICAL'], ['b', 'NUMERICAL'], ['c', 'NUMERICAL'],
        ['DEPENDENT', ['a', ',', 'b']],
        ['INDEPENDENT', ['b', ',', 'c']]]
    parsed = cgschema.parse(schema, False)
    expected = cgschema.GeneratorSchema(
        guess=False, subsample=False,
        columns=[('a', 'NUMERICAL'), ('b', 'NUMERICAL'), ('c', 'NUMERICAL')],
        dep_constraints=[(['a', 'b'], True), (['b', 'c'], False)])
    assert parsed == expected


def test_parses_guess_and_subsample():
    schema = [['GUESS', ['*']], ['SUBSAMPLE', [5]]]
    parsed = cgschema.parse(schema, True)
    expected = cgschema.GeneratorSchema(
        guess=True, subsample=5, columns=[], dep_constraints=[])
    assert parsed == expected


def test_parses_subsample_off():
    schema = [['GUESS', ['*']], ['SUBSAMPLE', ['OFF']]]
    parsed = cgschema.parse(schema, True)
    expected = cgschema.GeneratorSchema(
        guess=True, subsample=False, columns=[], dep_constraints=[])
    assert parsed == expected
