SCENARIOS = [
    {
        "room": "🕷️ Spider Cave",
        "description": "Thick webs cover every surface. Thousands of eyes glitter in the darkness. A giant shadow moves above you.",
        "difficulty": 0.4,
        "options": [
            {
                "name": "Stay perfectly still",
                "description": "Don't move a muscle. Spiders hunt by vibration.",
                "survival_score": 0.9
            },
            {
                "name": "Run through the webs",
                "description": "Sprint straight through to the exit.",
                "survival_score": 0.2
            },
            {
                "name": "Use your lighter to burn the webs",
                "description": "Fire will clear a path quickly.",
                "survival_score": 0.5
            }
        ]
    },
    {
        "room": "👻 Phantom Ballroom",
        "description": "Translucent figures waltz to silent music. One ghost turns and beckons you to join them.",
        "difficulty": 0.3,
        "options": [
            {
                "name": "Dance with the ghosts",
                "description": "Accept the invitation and follow their movements.",
                "survival_score": 0.85
            },
            {
                "name": "Run past them",
                "description": "Ignore them and rush to the far door.",
                "survival_score": 0.3
            },
            {
                "name": "Close your eyes and walk through",
                "description": "If you can't see them, they can't see you.",
                "survival_score": 0.6
            }
        ]
    },
    {
        "room": "🔪 Butcher's Kitchen",
        "description": "Blood-stained cleavers hang on hooks. Fresh meat sits on the counter. Something is still moving in the freezer.",
        "difficulty": 0.7,
        "options": [
            {
                "name": "Hide in the pantry",
                "description": "Crouch behind the shelves and wait for it to pass.",
                "survival_score": 0.7
            },
            {
                "name": "Grab a cleaver to defend yourself",
                "description": "Arm yourself with the biggest blade.",
                "survival_score": 0.3
            },
            {
                "name": "Sneak around the counter",
                "description": "Stay low and quiet, avoid detection.",
                "survival_score": 0.8
            }
        ]
    },
    {
        "room": "🪞 Mirror Maze",
        "description": "Your reflection doesn't match your movements. In one mirror, you see yourself screaming. In another, you're already dead.",
        "difficulty": 0.5,
        "options": [
            {
                "name": "Follow the dead reflection",
                "description": "The dead version of you knows the way out.",
                "survival_score": 0.75
            },
            {
                "name": "Smash all the mirrors",
                "description": "Destroy them before they trap you.",
                "survival_score": 0.1
            },
            {
                "name": "Don't look at any reflections",
                "description": "Keep your eyes down and feel along the walls.",
                "survival_score": 0.9
            }
        ]
    },
    {
        "room": "🧛 Vampire's Crypt",
        "description": "Coffins line the walls. One lid is slowly opening. You hear breathing that isn't your own.",
        "difficulty": 0.8,
        "options": [
            {
                "name": "Hold the coffin lid shut",
                "description": "Use all your strength to keep it closed.",
                "survival_score": 0.2
            },
            {
                "name": "Present your garlic necklace",
                "description": "Hold up the garlic you found earlier.",
                "survival_score": 0.95
            },
            {
                "name": "Lie down in an empty coffin",
                "description": "Pretend to be one of them.",
                "survival_score": 0.4
            }
        ]
    },
    {
        "room": "🎃 Pumpkin Patch",
        "description": "Hundreds of carved pumpkins, all facing you. Their candles flicker in unison. The vines are crawling closer.",
        "difficulty": 0.4,
        "options": [
            {
                "name": "Blow out all the candles",
                "description": "Darkness might protect you.",
                "survival_score": 0.3
            },
            {
                "name": "Jump from pumpkin to pumpkin",
                "description": "Don't touch the ground where the vines are.",
                "survival_score": 0.85
            },
            {
                "name": "Carve your own face into a pumpkin",
                "description": "Become one with the patch.",
                "survival_score": 0.7
            }
        ]
    },
    {
        "room": "🦇 Belfry Tower",
        "description": "Thousands of bats hang from the rafters. The bell begins to toll at midnight. They're starting to wake.",
        "difficulty": 0.6,
        "options": [
            {
                "name": "Ring the bell faster",
                "description": "Confuse them with constant noise.",
                "survival_score": 0.4
            },
            {
                "name": "Slide down the bell rope",
                "description": "Quick escape before they swarm.",
                "survival_score": 0.8
            },
            {
                "name": "Scream as loud as you can",
                "description": "Scare them away with noise.",
                "survival_score": 0.15
            }
        ]
    },
    {
        "room": "💀 Bone Chapel",
        "description": "Walls made entirely of human skulls. A skeletal priest stands at the altar, turning to face you.",
        "difficulty": 0.7,
        "options": [
            {
                "name": "Kneel and pray",
                "description": "Show respect in this unholy place.",
                "survival_score": 0.85
            },
            {
                "name": "Steal bones for weapons",
                "description": "Arm yourself from the walls.",
                "survival_score": 0.1
            },
            {
                "name": "Walk backwards toward the exit",
                "description": "Never turn your back on the dead.",
                "survival_score": 0.7
            }
        ]
    },
    {
        "room": "🌙 Werewolf's Den",
        "description": "Claw marks gouged deep into the walls. The full moon shines through a broken skylight. You hear howling.",
        "difficulty": 0.9,
        "options": [
            {
                "name": "Howl back",
                "description": "Assert dominance over the pack.",
                "survival_score": 0.3
            },
            {
                "name": "Cover the skylight",
                "description": "Block out the moonlight.",
                "survival_score": 0.8
            },
            {
                "name": "Load your silver bullet",
                "description": "Ready your one shot.",
                "survival_score": 0.75
            }
        ]
    },
    {
        "room": "🕯️ Séance Chamber",
        "description": "Candles form a pentagram. A Ouija board spells out 'HELP US.' The planchette moves on its own toward 'GOODBYE.'",
        "difficulty": 0.5,
        "options": [
            {
                "name": "Move planchette to 'HELLO'",
                "description": "Restart the conversation.",
                "survival_score": 0.2
            },
            {
                "name": "Let it finish saying goodbye",
                "description": "Don't interrupt the ritual.",
                "survival_score": 0.9
            },
            {
                "name": "Flip the board over",
                "description": "End the séance immediately.",
                "survival_score": 0.4
            }
        ]
    },
    {
        "room": "🧟 Graveyard Shift",
        "description": "Fresh graves with disturbed earth. Pale hands burst through the soil. The dead are rising all around you.",
        "difficulty": 0.8,
        "options": [
            {
                "name": "Run between the headstones",
                "description": "Zigzag through the graveyard.",
                "survival_score": 0.5
            },
            {
                "name": "Play dead on the ground",
                "description": "They won't notice another corpse.",
                "survival_score": 0.7
            },
            {
                "name": "Head for the church",
                "description": "Holy ground is safe ground.",
                "survival_score": 0.9
            }
        ]
    },
    {
        "room": "🎭 Cursed Theater",
        "description": "The stage lights flicker on. Ghostly actors perform your death scene. The audience of shadows applauds.",
        "difficulty": 0.6,
        "options": [
            {
                "name": "Rewrite the ending",
                "description": "Jump on stage and change the script.",
                "survival_score": 0.85
            },
            {
                "name": "Join the audience",
                "description": "Sit down and become a shadow.",
                "survival_score": 0.3
            },
            {
                "name": "Cut the stage lights",
                "description": "End the performance early.",
                "survival_score": 0.6
            }
        ]
    }
]
