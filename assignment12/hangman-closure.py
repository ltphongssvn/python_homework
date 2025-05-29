def make_hangman(secret_word):
    """
    This is the closure factory - it creates a customized hangman game function
    for a specific secret word.
    
    Think of this as setting up a private game room with all the necessary
    equipment: the secret word and a notebook to track guesses.
    
    Args:
        secret_word (str): The word that players need to guess
        
    Returns:
        function: A hangman game function that remembers this specific word
                 and tracks guesses across multiple calls
    """
    # These variables become part of the closure - they're "captured"
    # by the inner function and will persist between calls
    guesses = []  # The memory of all letters guessed so far
    secret_word = secret_word.lower()  # Normalize to lowercase for consistency
    
    def hangman_closure(letter):
        """
        This is the inner function that forms the actual closure.
        It has access to both 'secret_word' and 'guesses' from the outer function,
        and it can modify the 'guesses' list across multiple calls.
        
        Think of this as the game master who remembers everything about
        this particular game session.
        
        Args:
            letter (str): The letter being guessed this turn
            
        Returns:
            bool: True if the word is completely guessed, False otherwise
        """
        # Normalize the guessed letter to lowercase
        letter = letter.lower()
        
        # Add this letter to the persistent list of guesses
        # This list survives between function calls thanks to the closure
        guesses.append(letter)
        
        # Build the display version of the word
        # Show guessed letters, hide unguessed letters with underscores
        display_word = ""
        for char in secret_word:
            if char in guesses:
                display_word += char
            else:
                display_word += "_"
        
        # Show the current state to the player
        print(f"Word: {display_word}")
        print(f"Guesses so far: {sorted(list(set(guesses)))}")  # Show unique guesses, sorted
        
        # Check if the word is completely guessed
        # This happens when every character in the secret word has been guessed
        all_letters_guessed = all(char in guesses for char in secret_word)
        
        if all_letters_guessed:
            print(f"🎉 Congratulations! You guessed the word: {secret_word}")
            return True
        else:
            # Count how many letters are still hidden
            remaining_letters = len([char for char in secret_word if char not in guesses])
            print(f"Keep going! {remaining_letters} letters remaining.")
            return False
    
    # This is the key moment: we return the inner function
    # The inner function carries the secret_word and guesses with it
    return hangman_closure


def demonstrate_closure_behavior():
    """
    This function demonstrates how closures maintain independent state.
    Each closure created by make_hangman() has its own private memory.
    """
    print("=" * 60)
    print("DEMONSTRATION: How Closures Maintain Independent State")
    print("=" * 60)
    
    # Create two different hangman games with different words
    # Each will have its own independent closure with separate memory
    game1 = make_hangman("python")
    game2 = make_hangman("closure")
    
    print("Playing with game1 (word: 'python'):")
    game1('p')  # Guess 'p' in the first game
    game1('y')  # Guess 'y' in the first game
    
    print("\nPlaying with game2 (word: 'closure'):")
    game2('c')  # Guess 'c' in the second game
    game2('l')  # Guess 'l' in the second game
    
    print("\nBack to game1:")
    game1('t')  # The first game still remembers 'p' and 'y'
    
    print("\nNotice how each game maintains its own separate memory!")
    print("This is the magic of closures - independent state preservation.")
    print("=" * 60)


def play_hangman_game():
    """
    This function implements a complete hangman game using the closure.
    It demonstrates how closures enable clean, stateful game logic
    without needing global variables or complex object structures.
    """
    print("\n🎮 Welcome to Hangman - Closure Edition! 🎮")
    print("=" * 50)
    
    # Get the secret word from the user
    # We'll hide their input to maintain the surprise for other players
    secret_word = input("Enter the secret word (letters only): ").strip()
    
    # Validate the input
    if not secret_word.isalpha():
        print("Please use only letters for the secret word.")
        return
    
    # Clear the screen to hide the secret word (simple approach)
    print("\n" * 3)  # Add some space to push the secret word off screen
    print("🎯 The word has been set! Let the guessing begin!")
    print(f"The word has {len(secret_word)} letters.")
    print("=" * 50)
    
    # Create the hangman game closure
    # This captures the secret word and creates the persistent game state
    game = make_hangman(secret_word)
    
    # Game loop - continue until the word is guessed
    guess_count = 0
    max_wrong_guesses = 6  # Traditional hangman allows 6 wrong guesses
    wrong_guesses = 0
    guessed_letters = set()
    
    while True:
        print(f"\n--- Guess #{guess_count + 1} ---")
        
        # Get the player's guess
        letter = input("Guess a letter: ").strip().lower()
        
        # Validate the input
        if len(letter) != 1 or not letter.isalpha():
            print("Please enter exactly one letter.")
            continue
            
        if letter in guessed_letters:
            print(f"You already guessed '{letter}'. Try a different letter.")
            continue
        
        # Record this guess
        guessed_letters.add(letter)
        guess_count += 1
        
        # Check if the letter is in the word (before calling the closure)
        if letter not in secret_word.lower():
            wrong_guesses += 1
            print(f"❌ '{letter}' is not in the word. Wrong guesses: {wrong_guesses}/{max_wrong_guesses}")
        else:
            print(f"✅ Good guess! '{letter}' is in the word.")
        
        # Use the closure to process the guess and update the game state
        word_complete = game(letter)
        
        # Check win condition
        if word_complete:
            print(f"\n🏆 Victory! You guessed the word in {guess_count} guesses!")
            print(f"Wrong guesses: {wrong_guesses}/{max_wrong_guesses}")
            break
        
        # Check lose condition
        if wrong_guesses >= max_wrong_guesses:
            print(f"\n💀 Game Over! You've used all {max_wrong_guesses} wrong guesses.")
            print(f"The word was: {secret_word}")
            break
        
        # Show remaining chances
        remaining_chances = max_wrong_guesses - wrong_guesses
        print(f"Remaining wrong guesses: {remaining_chances}")


def main():
    """
    Main program controller that demonstrates different aspects of closures
    and provides an interactive hangman game experience.
    """
    print("🐍 Python Closures: Hangman Edition 🐍")
    print("This program demonstrates how closures maintain persistent state")
    print("across multiple function calls, enabling elegant game logic.")
    
    while True:
        print("\n" + "=" * 60)
        print("Choose your adventure:")
        print("1. See closure demonstration (educational)")
        print("2. Play hangman game (interactive)")
        print("3. Exit")
        print("=" * 60)
        
        choice = input("Enter your choice (1-3): ").strip()
        
        if choice == '1':
            demonstrate_closure_behavior()
        elif choice == '2':
            play_hangman_game()
            
            # Ask if they want to play again
            play_again = input("\nDo you want to play another round? (y/n): ").strip().lower()
            if play_again != 'y':
                continue
        elif choice == '3':
            print("Thanks for exploring closures! Happy coding! 🚀")
            break
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")

# This ensures the main function only runs when the script is executed directly
# not when it's imported as a module
if __name__ == "__main__":
    main()


    
