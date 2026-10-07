package media

import "testing"

func TestNormalizeRetention(t *testing.T) {
	for _, test := range []struct{ input, want string }{
		{"", RetentionDisabled},
		{RetentionDisabled, RetentionDisabled},
		{RetentionBestEffort, RetentionBestEffort},
		{RetentionRequired, RetentionRequired},
	} {
		t.Run(test.input, func(t *testing.T) {
			got, err := NormalizeRetention(test.input)
			if err != nil || got != test.want {
				t.Fatalf("NormalizeRetention(%q) = %q, %v; want %q", test.input, got, err, test.want)
			}
		})
	}
	for _, input := range []string{"sometimes", "REQUIRED", " required", "disabled ", " "} {
		if got, err := NormalizeRetention(input); err == nil || got != "" {
			t.Errorf("NormalizeRetention(%q) = %q, %v; want validation failure", input, got, err)
		}
	}
}
