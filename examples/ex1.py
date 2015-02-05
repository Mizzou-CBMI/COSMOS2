from cosmos import Execution, Cosmos, rel, Recipe
import tools

def ex1_main(execution):
    ech = execution.add_stage([tools.Echo(tags=dict(word='hello'), out='{word}'), tools.Echo(tags=dict(word='world'))])
    cat = execution.add_stage(tools.Cat(dict(n=n, **t.tags), t, '{word}/{n}') for t in ech.tasks for n in [1, 2])
    wdc = execution.add_stage(tools.WordCount(t.tags, t, '{word}/{n}')
                      for t in cat.tasks)

    execution.run()

if __name__ == '__main__':
    cosmos = Cosmos('sqlite:///sqlite.db')
    cosmos.initdb()

    execution = cosmos.start('Example1', 'out/ex1', max_attempts=2, restart=True, skip_confirm=True)
    ex1_main(execution)
