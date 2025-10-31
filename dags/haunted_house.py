import random
from datetime import timedelta

from airflow.providers.standard.operators.hitl import HITLBranchOperator
from airflow.providers.standard.operators.smooth import SmoothOperator
from airflow.sdk import dag, task, chain, task_group

from include.scenarios import SCENARIOS


@dag(render_template_as_native_obj=True)
def haunted_house():

    @task(task_display_name="🏠 Generate Haunted House")
    def generate_haunted_house():
        return random.sample(SCENARIOS, 4)

    def generate_encounter(i):

        @task_group(group_id=f"encounter_{i}", group_display_name=f"👻 Room {i}")
        def encounter(i: int):
            _encounter = HITLBranchOperator(
                task_id="encounter",
                task_display_name=f"😱 Encounter {i}",
                subject=f"{{{{ ti.xcom_pull(task_ids='generate_haunted_house')[{i}]['room'] }}}}",
                trigger_rule="one_success",
                body=f"""
**You encountered the following room:**
{{{{ ti.xcom_pull(task_ids='generate_haunted_house')[{i}]['room'] }}}}

{{{{ ti.xcom_pull(task_ids='generate_haunted_house')[{i}]['description'] }}}}

**Choose wisely:**
- **Option 1:** {{{{ ti.xcom_pull(task_ids='generate_haunted_house')[{i}]['options'][0]['name'] }}}}:
{{{{ ti.xcom_pull(task_ids='generate_haunted_house')[{i}]['options'][0]['description'] }}}}
- **Option 2:** {{{{ ti.xcom_pull(task_ids='generate_haunted_house')[{i}]['options'][1]['name'] }}}}:
{{{{ ti.xcom_pull(task_ids='generate_haunted_house')[{i}]['options'][1]['description'] }}}}
- **Option 3:** {{{{ ti.xcom_pull(task_ids='generate_haunted_house')[{i}]['options'][2]['name'] }}}}:
{{{{ ti.xcom_pull(task_ids='generate_haunted_house')[{i}]['options'][2]['description'] }}}}
        """,
                options=["Option 1", "Option 2", "Option 3"],
                options_mapping={
                    "Option 1": f"encounter_{i}.fate_decision_0",
                    "Option 2": f"encounter_{i}.fate_decision_1",
                    "Option 3": f"encounter_{i}.fate_decision_2",
                },
                execution_timeout=timedelta(hours=4),
            )

            def fate_decision(j, scenario):

                @task(task_id=f"fate_decision_{j}", task_display_name=f"⁉️ Fate Decision - Option {j}")
                def fate_decision(j, scenario):
                    room_difficulty = scenario["difficulty"]

                    option = scenario["options"][j]
                    survival_score = option["survival_score"]

                    base_chance = survival_score * (1 - room_difficulty * 0.4)
                    random_factor = random.uniform(-0.15, 0.15)
                    final_score = base_chance + random_factor

                    passed = final_score > 0.5

                    print(f"Fate decision for {scenario['room']} is {'passed' if passed else 'failed'}")
                    if not passed:
                        raise Exception("YOU FAILED ❌!")

                return fate_decision(j, scenario)

            # noinspection PyTypeChecker
            chain(_encounter, [
                fate_decision(0, f"{{{{ ti.xcom_pull(task_ids='generate_haunted_house')[{i}] }}}}"),
                fate_decision(1, f"{{{{ ti.xcom_pull(task_ids='generate_haunted_house')[{i}] }}}}"),
                fate_decision(2, f"{{{{ ti.xcom_pull(task_ids='generate_haunted_house')[{i}] }}}}")
            ])

        return encounter(i)

    chain(
        generate_haunted_house(),
        generate_encounter(0),
        generate_encounter(1),
        generate_encounter(2),
        generate_encounter(3),
        SmoothOperator(task_id="end", task_display_name="✅ YOU SURVIVED!", trigger_rule="one_success")
    )

haunted_house()
