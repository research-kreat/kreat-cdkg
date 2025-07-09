from together import Together
client = Together(api_key = "96c5a0b0a97b3e4f86423735fe2580f7bad8063c7ddac4c22d2b0a11bdb6fdce")


def ask_stage(prompt, label, context=None):
    full_prompt = f"{prompt}\n"
    if context:
        full_prompt += f"Here is the context to consider:\n{context}\n"
    response = client.chat.completions.create(
        model = "deepseek-ai/DeepSeek-V3",
        messages = [
            {"role": "system",
             "content": f"You are an LLM that helps analyze user challenges across structured stages: Title, Abstract, Assumptions, Constraints, Root Cause, and Summarisation. Do not answer any questions that deviate from the topic."},
            {"role": "user", "content": full_prompt}
        ]
    )
    result = response.choices[0].message.content.strip()
    print(f"\n{label}:\n{result}")
    return result


def main():
    challenge = input("Please enter your challenge: ")

    title_prompt = f"Generate a concise, impactful title for this challenge:\n\"{challenge}\". Keep the title as one phrase only. Take the challenge and generate a sentence that's grammatically accurate to the title which will be the title and nothing else. Do not add a statement before or following it. Keep it non-bold and as plain text."
    title = ask_stage(title_prompt, "Title")

    abstract_prompt = f"Using the challenge and the title, write an abstract that explains the problem, its context, and significance.\nChallenge: {challenge}\nTitle: {title}. Do not add a statement before or following it. Keep it non-bold and as plain text."
    abstract = ask_stage(abstract_prompt, "Abstract")

    assumptions_prompt = f"Based on the abstract, list 3–5 key assumptions we are making:\nAbstract: {abstract}. Do not add a statement before or following it. Keep it non-bold and as plain text."
    assumptions = ask_stage(assumptions_prompt, "Assumptions")

    constraints_prompt = f"Given the assumptions, what are the key constraints in addressing this challenge?\nAssumptions: {assumptions}. Do not add a statement before or following it. Keep it non-bold and as plain text."
    constraints = ask_stage(constraints_prompt, "Constraints")

    root_prompt = f"Using the assumptions and constraints, perform a root cause analysis of the challenge in plain text with absolutely no bold characters. Give me only the root cause analysis as output without headings.:\nChallenge: {challenge}\nAssumptions: {assumptions}\nConstraints: {constraints}. Do not add a statement before or following it. Keep it non-bold and as plain text even while generating headings and sub-headings and pointers. Let there be no scope to have asterisks while reducing it to plain text format later. Do not generate a summary. Keep it strictly really short and plain-text."
    root_cause = ask_stage(root_prompt, "Root Cause Analysis")

    summary_prompt = f"Summarize everything into a concise insight, incorporating the title, abstract, assumptions, constraints, and root cause:\n\nTitle: {title}\nAbstract: {abstract}\nAssumptions: {assumptions}\nConstraints: {constraints}\nRoot Cause: {root_cause}. Do not add a statement before or following it or headings. Keep it non-bold and as plain text."
    summary = ask_stage(summary_prompt, "Final Summary")

    print("All steps complete!")


if __name__ == "__main__":
    main()
