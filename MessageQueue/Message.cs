namespace MessageQueue
{
    public class Message
    {
        private string _content = string.Empty;
        public string Content 
        { 
            get => _content; 
            set
            {
                _content = value;
                Created = DateTime.UtcNow;
            }
        }

        public DateTime Created { get; private set; }

        public override string ToString()
        {
            return $"{Content}";
        }
    }
}
